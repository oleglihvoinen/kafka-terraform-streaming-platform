from __future__ import annotations

import argparse
import json
from confluent_kafka import Consumer, Producer

from src.streaming import MessageMeta, dead_letter_envelope, decide


def run(bootstrap_servers: str, group_id: str, topic: str, dlq_topic: str) -> None:
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    dlq = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True, "acks": "all"})
    processed: set[str] = set()
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(msg.error())

            meta = MessageMeta(msg.topic(), msg.partition(), msg.offset())
            decision = decide(msg.value(), meta, processed)

            if decision.action == "skip_duplicate":
                consumer.commit(message=msg, asynchronous=False)
                continue

            if decision.action == "dead_letter":
                envelope = dead_letter_envelope(msg.value(), meta, decision.error or "")
                dlq.produce(
                    dlq_topic,
                    key=meta.idempotency_key,
                    value=json.dumps(envelope).encode("utf-8"),
                )
                dlq.flush()
                consumer.commit(message=msg, asynchronous=False)
                processed.add(meta.idempotency_key)
                continue

            # Replace this block with a transactional Snowflake/Fabric sink.
            print(json.dumps(decision.event, sort_keys=True))

            # Offset is committed only after accepted processing completes.
            consumer.commit(message=msg, asynchronous=False)
            processed.add(meta.idempotency_key)
    finally:
        consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--group-id", default="orders-sink-v1")
    parser.add_argument("--topic", default="orders.v1")
    parser.add_argument("--dlq-topic", default="orders.dlq.v1")
    args = parser.parse_args()
    run(args.bootstrap_servers, args.group_id, args.topic, args.dlq_topic)


if __name__ == "__main__":
    main()
