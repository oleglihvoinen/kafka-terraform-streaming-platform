# Kafka + Terraform Streaming Data Platform

A production-oriented streaming platform case that combines **Apache Kafka, Python and Terraform** with explicit event contracts, partitioning strategy, replay-safe consumption, dead-letter handling, offset discipline and Infrastructure as Code.

## Architecture

```text
Order/API producers
        |
        v
  orders.v1 topic
        |
        +----> validated consumer group ----> Snowflake/Fabric sink
        |                 |
        |                 +----> dead-letter topic
        |
        +----> independent operational / fraud consumers

Terraform -> cluster/topic configuration, retention, partitioning and environment variables
```

## Engineering focus

- versioned order-event contract
- deterministic Kafka message key for per-order ordering
- producer delivery acknowledgements
- consumer validation before offset commit
- replay-safe idempotency key built from topic/partition/offset
- dead-letter envelope preserving original payload and error
- explicit manual commit semantics
- Terraform-managed topic configuration
- unit tests for event validation and processing decisions
- CI for Python tests, JSON validation and Terraform formatting

The public implementation is local/environment-neutral. Cloud Kafka credentials and Snowflake/Fabric sinks are intentionally external.

## Run

```bash
python -m pip install -r requirements.txt
pytest -q
```

**Technologies:** Apache Kafka · confluent-kafka · Python · Terraform · JSON Schema · event contracts · DLQ · replay/idempotency · CI/CD · Snowflake/Fabric-ready ingestion
