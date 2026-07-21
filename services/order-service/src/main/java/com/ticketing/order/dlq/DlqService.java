package com.ticketing.order.dlq;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Read + replay helper for the dead-letter topics ({@code <source>.DLQ}).
 *
 * <p><b>Why this exists.</b> Every saga consumer routes a record that exhausts its
 * retries to {@code <topic>.DLQ} (see note 05 + {@code KafkaConsumerConfig}). Until
 * now nothing ever <i>read</i> those topics — a DLQ you can't inspect or replay is
 * just a silent drop with extra steps. This service gives the debug dashboard three
 * operations against ANY {@code *.DLQ} topic in the cluster:
 * <ol>
 *   <li>{@link #listDlqTopics()} — which DLQs exist and how many messages each holds,</li>
 *   <li>{@link #peek(String, int)} — read messages WITHOUT committing (a non-destructive
 *       look: the record stays on the DLQ),</li>
 *   <li>{@link #replay(String, Integer, Long)} — republish record(s) back onto the
 *       source topic so the original consumer re-processes them.</li>
 * </ol>
 *
 * <p><b>Why replay is safe.</b> Republishing re-delivers an event a consumer may have
 * already partly handled. That is fine here because every saga consumer is idempotent
 * (dedup key + processed-events store, note 05): re-processing a message it has already
 * applied is a no-op. Replay is the <i>reward</i> for having built idempotent consumers.
 *
 * <p><b>Poison messages and why naive replay compounds.</b> A Kafka topic is an append-only
 * log; "replay" copies a record forward, it does not delete it. A truly <i>poison</i> record
 * (malformed, will never parse) fails again on the source topic and the recoverer re-lands it
 * on the DLQ — so a blind "replay everything from the beginning" GROWS the DLQ every click
 * (2 → 4 → 8 ...) and re-drives the same doomed record forever. Two things prevent that here:
 * <ol>
 *   <li><b>Consume-once.</b> Replay reads with a durable consumer group and COMMITS offsets,
 *       so each drain starts after what the last one handled instead of re-reading history.
 *       Without this, the immutable attempts=0 record is re-read (and re-emitted) on every
 *       call and the attempt cap below can never take effect.</li>
 *   <li><b>Attempt cap + parking-lot.</b> Each replay stamps/increments an
 *       {@code x-replay-attempts} header (the recoverer preserves it when a record re-lands).
 *       Once a record has been re-driven {@link #MAX_REPLAY_ATTEMPTS} times it is routed to a
 *       terminal {@code <topic>.parking} queue instead of back to the source — quarantined,
 *       never replayed again. That is what stops the compounding.</li>
 * </ol>
 * A transient-failure victim (downstream was briefly down) succeeds on replay and never comes
 * back. A poison record burns its attempts, then parks. Either way the DLQ converges.
 *
 * <p><b>Implementation note.</b> We open a short-lived AdminClient / KafkaConsumer per call
 * and close it, rather than holding long-lived beans. This tool is invoked rarely (a human
 * clicking in a dashboard), so simplicity + statelessness beats connection reuse here. The
 * peek consumer uses a random group id and {@code assign()} (not {@code subscribe()}), so it
 * never joins the real consumer groups and never commits offsets — it cannot disturb the
 * live saga.
 */
@Service
public class DlqService {

    private static final Logger log = LoggerFactory.getLogger(DlqService.class);

    /** Our DLQ naming convention (mirrors {@code Topics.dlq()} / the DeadLetterPublishingRecoverer). */
    private static final String DLQ_SUFFIX = ".DLQ";

    /** Terminal quarantine for records that exhausted their replay attempts. */
    private static final String PARKING_SUFFIX = ".parking";

    /** Header carrying how many times a record has been re-driven from a DLQ. */
    private static final String REPLAY_ATTEMPTS_HEADER = "x-replay-attempts";

    /** Why a record was parked (stamped on the parking-lot record for humans). */
    private static final String PARKED_REASON_HEADER = "x-parked-reason";

    /**
     * How many times a record may be re-driven before it is parked. Kept low so a
     * genuine poison message is quarantined quickly instead of thrashing the saga.
     * A transient victim normally succeeds on the first replay and never returns.
     */
    static final int MAX_REPLAY_ATTEMPTS = 3;

    /**
     * Durable group id for the replay consumer. Fixed (not random) ON PURPOSE: its
     * committed offsets are what give replay consume-once semantics across calls.
     */
    private static final String REPLAY_GROUP = "dlq-replay";

    /** Safety cap so a huge DLQ can't OOM the peek/replay in one shot. */
    private static final int MAX_PEEK = 500;

    private final KafkaAdmin kafkaAdmin;
    private final KafkaTemplate<String, String> kafka;

    public DlqService(KafkaAdmin kafkaAdmin, KafkaTemplate<String, String> kafka) {
        this.kafkaAdmin = kafkaAdmin;
        this.kafka = kafka;
    }

    // ------------------------------------------------------------------
    // 1) List DLQ topics + their depths
    // ------------------------------------------------------------------

    /**
     * All {@code *.DLQ} topics in the cluster with a live message count.
     *
     * <p>Count = sum over partitions of {@code endOffset - beginningOffset}. On a
     * never-compacted topic that equals the number of records currently retained,
     * which is exactly "how deep is this dead-letter queue".
     */
    public List<DlqTopic> listDlqTopics() {
        List<DlqTopic> result = new ArrayList<>();
        // AdminClient lists topic names; we filter to the DLQ convention.
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties());
             KafkaConsumer<String, String> consumer = newPeekConsumer()) {
            List<String> dlqNames = admin.listTopics().names().get().stream()
                    .filter(name -> name.endsWith(DLQ_SUFFIX))
                    .sorted()
                    .toList();
            for (String topic : dlqNames) {
                long count = countMessages(consumer, topic);
                result.add(new DlqTopic(topic, sourceTopicOf(topic), count));
            }
        } catch (Exception e) {
            // Surface as a runtime error → the controller maps it to a 500 the
            // dashboard can show. Don't swallow: a broken DLQ view should be loud.
            throw new IllegalStateException("Failed to list DLQ topics: " + e.getMessage(), e);
        }
        return result;
    }

    /** Depth of one topic = Σ(endOffset − beginningOffset) across its partitions. */
    private long countMessages(KafkaConsumer<String, String> consumer, String topic) {
        List<TopicPartition> tps = partitionsOf(consumer, topic);
        if (tps.isEmpty()) {
            return 0;
        }
        Map<TopicPartition, Long> begin = consumer.beginningOffsets(tps);
        Map<TopicPartition, Long> end = consumer.endOffsets(tps);
        long total = 0;
        for (TopicPartition tp : tps) {
            total += end.getOrDefault(tp, 0L) - begin.getOrDefault(tp, 0L);
        }
        return total;
    }

    // ------------------------------------------------------------------
    // 2) Peek (non-destructive read)
    // ------------------------------------------------------------------

    /**
     * Read up to {@code max} messages from a DLQ topic without committing offsets.
     * The DeadLetterPublishingRecoverer stamps the original failure onto Kafka
     * headers ({@code kafka_dlt-*}); we surface the exception message + original
     * topic so you can see WHY each record died.
     */
    public List<DlqMessage> peek(String topic, int max) {
        requireDlq(topic);
        int limit = Math.min(max <= 0 ? MAX_PEEK : max, MAX_PEEK);
        List<DlqMessage> out = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = newPeekConsumer()) {
            List<TopicPartition> tps = partitionsOf(consumer, topic);
            if (tps.isEmpty()) {
                return out;
            }
            // assign (not subscribe) + seekToBeginning → read the whole log from the
            // start, join no group, commit nothing. A pure observer.
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            Map<TopicPartition, Long> end = consumer.endOffsets(tps);

            long deadline = System.currentTimeMillis() + 3000; // don't block the request forever
            while (out.size() < limit && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> rec : records) {
                    out.add(toMessage(rec));
                    if (out.size() >= limit) {
                        break;
                    }
                }
                // Stop once every partition's read position has caught up to its end.
                boolean drained = tps.stream()
                        .allMatch(tp -> consumer.position(tp) >= end.getOrDefault(tp, 0L));
                if (drained) {
                    break;
                }
            }
        }
        return out;
    }

    // ------------------------------------------------------------------
    // 3) Replay (republish to the source topic)
    // ------------------------------------------------------------------

    /**
     * Re-drive DLQ record(s): send each back to its source topic, or — once it has
     * exhausted {@link #MAX_REPLAY_ATTEMPTS} — to the terminal {@code <topic>.parking}
     * queue so it stops circulating.
     *
     * <p>Bulk mode (no {@code partition}/{@code offset}) reads with a durable,
     * offset-committing consumer, so repeated calls drain FORWARD rather than
     * re-reading the whole DLQ — that consume-once behaviour is what lets the
     * attempt counter actually climb and eventually park a poison record. Targeted
     * mode ({@code partition}+{@code offset}) re-drives exactly one record (e.g. a
     * transient victim after you've fixed the cause) and does not move the group's
     * committed position.
     *
     * @return counts of records re-emitted to source vs. parked
     */
    public ReplayResult replay(String topic, Integer partition, Long offset) {
        requireDlq(topic);
        String sourceTopic = sourceTopicOf(topic);
        String parkingTopic = topic + PARKING_SUFFIX;
        boolean single = partition != null && offset != null;

        int replayed = 0;
        int parked = 0;

        try (KafkaConsumer<String, String> consumer = newReplayConsumer(single)) {
            List<TopicPartition> tps = partitionsOf(consumer, topic);
            if (tps.isEmpty()) {
                return new ReplayResult(topic, sourceTopic, 0, 0);
            }
            consumer.assign(tps);
            if (single) {
                consumer.seek(new TopicPartition(topic, partition), offset);
            } else {
                seekToCommittedOrBeginning(consumer, tps);
            }
            Map<TopicPartition, Long> end = consumer.endOffsets(tps);

            long deadline = System.currentTimeMillis() + 5000;
            boolean done = false;
            while (!done && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> rec : records) {
                    if (single && !(rec.partition() == partition && rec.offset() == offset)) {
                        continue; // targeting one record → ignore the rest
                    }
                    if (forward(rec, sourceTopic, parkingTopic)) {
                        parked++;
                    } else {
                        replayed++;
                    }
                    if (single) {
                        done = true;
                        break;
                    }
                }
                if (!single) {
                    boolean drained = tps.stream()
                            .allMatch(tp -> consumer.position(tp) >= end.getOrDefault(tp, 0L));
                    if (drained) {
                        done = true;
                    }
                }
            }
            // Commit ONLY for a bulk drain: that's what makes the next call resume
            // after these records instead of re-reading them. A targeted single
            // replay is a manual one-off and must not move the shared group.
            if (!single) {
                consumer.commitSync();
            }
        }

        log.info("Replay of {}: {} re-emitted to {}, {} parked to {}{}",
                topic, replayed, sourceTopic, parked, topic + PARKING_SUFFIX,
                parked > 0 ? " (exhausted " + MAX_REPLAY_ATTEMPTS + " attempts)" : "");
        return new ReplayResult(topic, sourceTopic, replayed, parked);
    }

    /**
     * Forward one DLQ record. Returns true if it was PARKED (attempts exhausted),
     * false if it was re-emitted to the source topic with an incremented counter.
     */
    private boolean forward(ConsumerRecord<String, String> rec, String sourceTopic, String parkingTopic) {
        int attempts = attemptsOf(rec);
        try {
            if (shouldPark(attempts)) {
                ensureTopic(parkingTopic);   // auto-create is off broker-side; make it on demand
                List<Header> headers = List.of(
                        attemptsHeader(attempts),
                        new RecordHeader(PARKED_REASON_HEADER,
                                ("exhausted " + MAX_REPLAY_ATTEMPTS + " replay attempts")
                                        .getBytes(StandardCharsets.UTF_8)));
                send(parkingTopic, rec.key(), rec.value(), headers);
                return true;
            }
            // Re-emit with attempts+1 and the ORIGINAL key, so it lands on the same
            // partition (per-order ordering) and the recoverer will carry the higher
            // count onto the record if it dead-letters again.
            send(sourceTopic, rec.key(), rec.value(), List.of(attemptsHeader(attempts + 1)));
            return false;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Replay failed for " + rec.topic() + "@" + rec.partition() + ":" + rec.offset()
                            + ": " + e.getMessage(), e);
        }
    }

    /** True once a record has been re-driven the maximum number of times. */
    static boolean shouldPark(int attempts) {
        return attempts >= MAX_REPLAY_ATTEMPTS;
    }

    /** Current replay-attempt count on a record (0 if the header is absent/garbage). */
    private int attemptsOf(ConsumerRecord<String, String> rec) {
        return parseAttempts(header(rec, REPLAY_ATTEMPTS_HEADER));
    }

    /** Parse the attempts header value; anything non-numeric counts as 0. */
    static int parseAttempts(String headerValue) {
        if (headerValue == null) {
            return 0;
        }
        try {
            return Math.max(0, Integer.parseInt(headerValue.trim()));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private Header attemptsHeader(int n) {
        return new RecordHeader(REPLAY_ATTEMPTS_HEADER,
                Integer.toString(n).getBytes(StandardCharsets.UTF_8));
    }

    /** Send one record (with headers) to a topic and block on the ack. */
    private void send(String topic, String key, String value, List<Header> headers) throws Exception {
        kafka.send(new ProducerRecord<>(topic, null, key, value, headers)).get();
    }

    /**
     * Create a topic if it doesn't exist. The broker has auto-create disabled, so
     * the parking topic must be made explicitly the first time anything is parked.
     * Idempotent: an already-existing topic is fine.
     */
    private void ensureTopic(String name) {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            admin.createTopics(List.of(new NewTopic(name, 3, (short) 1))).all().get();
            log.info("Created parking topic {}", name);
        } catch (Exception e) {
            if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                throw new IllegalStateException("Could not ensure topic " + name + ": " + e.getMessage(), e);
            }
        }
    }

    /** Resume a bulk drain from committed offsets; partitions with none start at the beginning. */
    private void seekToCommittedOrBeginning(KafkaConsumer<String, String> consumer, List<TopicPartition> tps) {
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(new HashSet<>(tps));
        List<TopicPartition> noCommit = new ArrayList<>();
        for (TopicPartition tp : tps) {
            OffsetAndMetadata om = committed.get(tp);
            if (om != null) {
                consumer.seek(tp, om.offset());
            } else {
                noCommit.add(tp);
            }
        }
        if (!noCommit.isEmpty()) {
            consumer.seekToBeginning(noCommit);
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    /** Map a raw Kafka record + its dead-letter headers into our DTO. */
    private DlqMessage toMessage(ConsumerRecord<String, String> rec) {
        return new DlqMessage(
                rec.partition(),
                rec.offset(),
                rec.key(),
                rec.value(),
                header(rec, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                header(rec, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                rec.timestamp());
    }

    /** Read one dead-letter header as a UTF-8 string (null if absent). */
    private String header(ConsumerRecord<String, String> rec, String key) {
        Header h = rec.headers().lastHeader(key);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    /** All partitions of a topic as TopicPartitions (empty list if the topic is unknown). */
    private List<TopicPartition> partitionsOf(KafkaConsumer<String, String> consumer, String topic) {
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        if (infos == null) {
            return List.of();
        }
        List<TopicPartition> tps = new ArrayList<>(infos.size());
        for (PartitionInfo info : infos) {
            tps.add(new TopicPartition(topic, info.partition()));
        }
        return tps;
    }

    /**
     * Consumer for replay. Bulk drains use the fixed {@link #REPLAY_GROUP} so their
     * committed offsets persist across calls (consume-once); a targeted single
     * replay uses a random group because it deliberately doesn't move that position.
     */
    private KafkaConsumer<String, String> newReplayConsumer(boolean single) {
        Map<String, Object> cfg = kafkaAdmin.getConfigurationProperties();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.get("bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                single ? "dlq-replay-single-" + UUID.randomUUID() : REPLAY_GROUP);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    /** A throwaway consumer for observing: random group, no auto-commit, string (de)ser. */
    private KafkaConsumer<String, String> newPeekConsumer() {
        Map<String, Object> cfg = kafkaAdmin.getConfigurationProperties();
        Properties props = new Properties();
        // bootstrap.servers comes from the shared KafkaAdmin config (same broker).
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                cfg.get("bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-peek-" + UUID.randomUUID());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    /** {@code orders.placed.DLQ} → {@code orders.placed}. */
    private String sourceTopicOf(String dlqTopic) {
        return dlqTopic.substring(0, dlqTopic.length() - DLQ_SUFFIX.length());
    }

    /** Guard: every operation here is only meaningful against a real DLQ topic. */
    private void requireDlq(String topic) {
        if (topic == null || !topic.endsWith(DLQ_SUFFIX)) {
            throw new IllegalArgumentException("Not a DLQ topic (must end in " + DLQ_SUFFIX + "): " + topic);
        }
    }

    // ------------------------------------------------------------------
    // DTOs (records → clean JSON for the dashboard)
    // ------------------------------------------------------------------

    /** One dead-letter topic and how many records it currently holds. */
    public record DlqTopic(String topic, String sourceTopic, long messageCount) {
    }

    /** One dead-lettered record, with the failure info the recoverer stamped on. */
    public record DlqMessage(
            int partition,
            long offset,
            String key,
            String value,
            String exceptionMessage,
            String originalTopic,
            long timestamp) {
    }

    /**
     * Outcome of a replay call.
     *
     * @param replayed records re-emitted to the source topic (attempts remaining)
     * @param parked   records quarantined to {@code <topic>.parking} (attempts exhausted)
     */
    public record ReplayResult(String dlqTopic, String sourceTopic, int replayed, int parked) {
    }
}
