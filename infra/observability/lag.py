#!/usr/bin/env python3
"""Print Kafka consumer-group lag, read from Prometheus. Used by `make lag`.

Lag is the health metric for an event-driven system: it is the number of
messages a consumer group has NOT processed yet.

    lag = log-end-offset (last message produced)
        - committed offset (last message the group finished)

Reading it:
  * lag 0            consumers are caught up
  * lag > 0, falling a burst is draining — normal, self-healing
  * lag > 0, rising  consumers cannot keep up; this is the real alert
  * lag stuck        consumer is wedged or dead (nothing is committing)

The last case is why this comes from the broker (via kafka-exporter) and not
from the services themselves: a crashed consumer publishes no metrics at all,
and that is exactly when its lag matters most.
"""

import json
import sys
import urllib.parse
import urllib.request

PROMETHEUS = "http://localhost:9090"
# Sum per group+topic: the per-partition detail is noise until you're actually
# chasing a hot partition.
QUERY = "sum by (consumergroup, topic) (kafka_consumergroup_lag_sum)"


def main() -> int:
    url = f"{PROMETHEUS}/api/v1/query?" + urllib.parse.urlencode({"query": QUERY})
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            result = json.load(response)["data"]["result"]
    except Exception as exc:
        print(f"could not reach Prometheus at {PROMETHEUS}: {exc}")
        print("start it with: make obs")
        return 1

    if not result:
        print("no lag series yet — is kafka-exporter running? (make obs)")
        return 0

    # Busiest first: if something is falling behind, it should be line one.
    rows = sorted(result, key=lambda m: -float(m["value"][1]))
    print(f"{'GROUP':<24} {'TOPIC':<30} {'LAG':>8}")
    for metric in rows:
        labels = metric["metric"]
        lag = int(float(metric["value"][1]))
        print(f"{labels.get('consumergroup', '?'):<24} "
              f"{labels.get('topic', '?'):<30} {lag:>8}")

    total = sum(int(float(m["value"][1])) for m in rows)
    print(f"{'':<24} {'TOTAL':<30} {total:>8}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
