package com.ticketing.order.dlq;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
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
import java.util.HashMap;
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
 * <p><b>What replay does NOT do.</b> A Kafka topic is an append-only log; "replay" copies
 * a record forward, it does not delete it from the DLQ. So {@link #peek} keeps showing a
 * replayed message. A production system would track a per-record "replayed" marker (or use
 * a compacted status topic); for this learning/debug tool we keep it stateless and just
 * re-emit. True <i>poison</i> messages (malformed, will never parse) simply re-land on the
 * DLQ when replayed — correct behaviour: poison is fix-then-replay or discard, never blind
 * infinite replay.
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
     * Republish DLQ record(s) onto their source topic so the original consumer
     * re-processes them.
     *
     * @param topic     the {@code *.DLQ} topic
     * @param partition if non-null together with {@code offset}, replay ONLY that one
     *                  record; otherwise replay every record currently on the DLQ
     * @param offset    see {@code partition}
     * @return how many records were re-emitted (and to where)
     */
    public ReplayResult replay(String topic, Integer partition, Long offset) {
        requireDlq(topic);
        String sourceTopic = sourceTopicOf(topic);

        // Reuse peek to gather the candidate records (already a non-destructive read).
        List<DlqMessage> candidates = peek(topic, MAX_PEEK);
        boolean single = partition != null && offset != null;

        int replayed = 0;
        for (DlqMessage msg : candidates) {
            if (single && !(msg.partition() == partition && msg.offset() == offset)) {
                continue; // targeting one record → skip the rest
            }
            // Re-send with the ORIGINAL key so the event lands on the same partition
            // and keeps its per-order ordering. Blocking .get() surfaces send failures.
            try {
                kafka.send(sourceTopic, msg.key(), msg.value()).get();
                replayed++;
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Replay failed for " + topic + "@" + msg.partition() + ":" + msg.offset()
                                + " → " + sourceTopic + ": " + e.getMessage(), e);
            }
        }
        log.info("Replayed {} record(s) from {} back to {}", replayed, topic, sourceTopic);
        return new ReplayResult(topic, sourceTopic, replayed);
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

    /** Outcome of a replay call. */
    public record ReplayResult(String dlqTopic, String sourceTopic, int replayed) {
    }
}
