# Flash-Sale Ticketing Saga Platform

An event-driven backend that demonstrates **distributed-systems patterns done
correctly under failure**. Buying a ticket kicks off an asynchronous **saga** across
four Spring Boot services that coordinate entirely through Apache Kafka — no shared
database, no synchronous calls for state changes.

The ticketing domain is deliberately just the excuse. The real subject is
event-driven architecture that stays correct when the network, the broker, and its
own services fail. Every pattern here earns its place by preventing a specific,
nameable failure.

> **Tech:** Java 21 · Spring Boot 3 · Apache Kafka (KRaft) · PostgreSQL (one per
> service) · Redis · Resilience4j · Flyway · Docker Compose · OpenTelemetry +
> Prometheus + Grafana + Jaeger.

---

## The saga

Four services, each owning a **private** PostgreSQL database (database-per-service,
no shared tables). Cross-service state moves *only* via Kafka events.

```
OrderPlaced ─▶ SeatReserved ─▶ PaymentAuthorized ─▶ OrderConfirmed ─▶ notification
 (order)        (inventory)      (payment)            (order)
```

On failure, **compensation runs backward** — e.g. a declined payment releases the
seat, cancels the order, and refunds if already charged.

| Service | Owns | Port |
|---|---|---|
| order-service | Order lifecycle; accepts orders; drives the saga's terminal state | 8081 |
| inventory-service | Seat inventory; reserves/releases specific seats | 8082 |
| payment-service | Authorize / decline / refund | 8083 |
| notification-service | Order-status notifications | 8084 |

---

## Distributed-systems patterns implemented

| Pattern | What it prevents |
|---|---|
| **Transactional outbox** | The dual-write problem — DB and Kafka can't desynchronize on a crash. |
| **Idempotent consumers** | At-least-once delivery means duplicates *will* arrive; reprocessing is a no-op. |
| **Idempotent entry point** | A retried/double-clicked order request returns the same order, not a new one (Postgres advisory lock). |
| **Saga + compensation** | A multi-service transaction with no distributed lock; failures unwind backward. |
| **Redis distributed lock** | Two buyers can't reserve the same seat across service instances (`SET NX` + token'd Lua release). |
| **Redis cache-aside** | Hot seat reads served from cache; the reservation *decision* still reads source-of-truth under the lock. |
| **Circuit breaker + retry** | A flaky payment gateway can't cascade into the whole service (Resilience4j). |
| **DLQ + replay + parking-lot** | Poison messages are quarantined, not dropped or infinitely looped. |
| **Distributed tracing** | One order = one trace across all four services (W3C `traceparent` over Kafka). |
| **Consumer-lag monitoring** | The health metric for an EDA — read broker-side so a *dead* consumer still reports. |
| **Invariant checker / reconciler** | Detects cross-service divergence a saga can't structurally prevent. |

---

## Correct under failure — proven, not claimed

Failure paths were driven deliberately, and doing so **surfaced real bugs** that were
then fixed — which is the whole point:

- **Non-idempotent entry point** — the same request id created duplicate orders under
  retry; fixed with an advisory lock + unique-index backstop.
- **DLQ replay compounding** — replaying a poison message grew the queue forever;
  fixed with consume-once offsets + an attempt cap that parks exhausted records.
- **Silent tracing breakage** — the outbox published on a thread with no trace
  context (saga shattered into unrelated traces); fixed by persisting the trace parent
  in the outbox row.

The invariant checker even caught a cross-database inconsistency introduced by
accident during testing — a reconciler doing exactly its job.

---

## Running it

Everything runs locally on Docker — no paid cloud.

```bash
cp .env.example .env

# 1. Core infra: Kafka, 4x Postgres, Redis
make up-core

# 2. Observability: Prometheus, Grafana, OTel collector, Jaeger, kafka-exporter
make obs

# 3a. Run the four services on the host (fast dev loop)
make run-order      # + run-inventory / run-payment / run-notification
# 3b. ...or as containers, no host JDK needed
make up-apps

# 4. Seed demo seats (idempotent, safe to re-run)
make seed
```

Then drive and observe it:

```bash
# place an order
curl -X POST localhost:8081/api/orders -H 'Content-Type: application/json' \
  -d '{"userId":"u1","seatId":"seat-A1","eventScheduleId":"show-1","amount":50.00,"currency":"USD","requestId":"r1"}'

make check          # cross-service invariant check
make lag            # Kafka consumer-group lag
make trace          # open Jaeger — one order as one trace
make grafana        # open the saga dashboard

# chaos
make gateway-fail   # trip the payment circuit breaker
make poison         # inject a poison message → DLQ
```

Run `make help` for the full target list. A thin read-only debug dashboard
(`dashboard/index.html`) lists orders and their status.

---

## Scope

Deliberately kept lean and $0-local. Notably **not** built (and why): Kubernetes/HPA
autoscaling (scaling is already shown by partitions + consumer groups), a load
generator, a chaos-control UI (the Make targets inject failures directly), and a saga
visualizer (Jaeger shows it better).
