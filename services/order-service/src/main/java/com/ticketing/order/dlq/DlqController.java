package com.ticketing.order.dlq;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Debug/ops REST surface over the dead-letter topics, backing the dashboard's DLQ
 * panel. order-service hosts it because it's the saga's front door and already has
 * a {@code KafkaTemplate<String,String>}; the DLQ topics themselves live in Kafka,
 * so ONE admin surface here can inspect/replay every {@code *.DLQ} in the cluster
 * (no need to duplicate this in all four services).
 *
 * <p>All routes are read-mostly except {@code /replay}, which re-emits events — safe
 * because saga consumers are idempotent (see {@link DlqService}).
 */
@RestController
@RequestMapping("/api/dlq")
public class DlqController {

    private final DlqService dlq;

    public DlqController(DlqService dlq) {
        this.dlq = dlq;
    }

    /** GET /api/dlq/topics — every DLQ topic + its current depth. */
    @GetMapping("/topics")
    public List<DlqService.DlqTopic> topics() {
        return dlq.listDlqTopics();
    }

    /**
     * GET /api/dlq/{topic}?max=50 — peek messages on a DLQ without consuming them.
     * {@code topic} is the full DLQ name, e.g. {@code orders.placed.DLQ}.
     */
    @GetMapping("/{topic}")
    public List<DlqService.DlqMessage> peek(@PathVariable String topic,
                                            @RequestParam(defaultValue = "50") int max) {
        return dlq.peek(topic, max);
    }

    /**
     * POST /api/dlq/{topic}/replay — republish DLQ records to their source topic.
     *
     * <p>With no query params it replays the WHOLE DLQ. Pass {@code partition} +
     * {@code offset} to replay exactly one record (e.g. after you've fixed the bug
     * that poisoned it, or to re-drive a single transient-failure victim).
     */
    @PostMapping("/{topic}/replay")
    public DlqService.ReplayResult replay(@PathVariable String topic,
                                          @RequestParam(required = false) Integer partition,
                                          @RequestParam(required = false) Long offset) {
        return dlq.replay(topic, partition, offset);
    }
}
