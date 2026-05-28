# ADR-001: Kafka KRaft mode over Zookeeper

**Status:** Accepted | **Date:** 2024-05

## Context
Needed a message broker for high-throughput transaction streaming. Considered: Kafka with Zookeeper, Kafka KRaft, RabbitMQ, Redis Streams.

## Decision
Use Apache Kafka 3.7 in KRaft mode (no Zookeeper dependency).

## Reasons
- KRaft removes the operational overhead of managing a separate Zookeeper cluster — one fewer service to monitor and restart
- Kafka 3.3+ KRaft is production-stable and the future default for all Kafka deployments
- Log-based storage enables message replay — if Spark crashes, it resumes from the last checkpoint offset without data loss
- 4 partitions on ingestion topics allow Spark to parallelize consumption across workers

## Trade-offs accepted
- KRaft controller quorum adds complexity vs single-node Zookeeper for development
- Some observability tooling has slightly less mature KRaft support
- RabbitMQ would be simpler for low-throughput use cases — Kafka is over-engineered below 10K msg/s but correct for financial pipelines
