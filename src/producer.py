from __future__ import annotations

import argparse
import json
from confluent_kafka import Producer

from src.streaming import validate_event


def publish(bootstrap_servers: str, topic: str, event: dict) -> None:
    validate_event(json.dumps(event))
    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
    })

    delivery_error = []

    def delivered(err, msg):
        if err:
            delivery_error.append(str(err))
        else:
            print(f"delivered {msg.topic()}[{msg.partition()}]@{msg.offset()}")

    producer.produce(
        topic=topic,
        key=event["order_id"],
        value=json.dumps(event).encode("utf-8"),
        on_delivery=delivered,
    )
    producer.flush()
    if delivery_error:
        raise RuntimeError(delivery_error[0])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--topic", default="orders.v1")
    parser.add_argument("--event-json", required=True)
    args = parser.parse_args()
    publish(args.bootstrap_servers, args.topic, json.loads(args.event_json))


if __name__ == "__main__":
    main()
