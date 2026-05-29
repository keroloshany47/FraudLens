# ADR-001 — Kafka in KRaft Mode (No Zookeeper)

| Field       | Value                          |
|-------------|--------------------------------|
| **Status**  | Accepted                       |
| **Date**    | 2024-01                        |
| **Decider** | FraudLens Engineering          |
| **Layer**   | Message Broker / Ingestion     |

---

## Context

FraudLens requires a message broker to decouple the Python Kafka producer
(which replays `fraudTest.csv` in real time) from the Spark Structured
Streaming consumer. The broker must:

- Handle sustained throughput of 550K messages replayed at 0.05 s/message
- Support at-least-once delivery with `acks=all`
- Run reliably inside a single-machine Docker Compose stack without inflating
  the service count unnecessarily
- Be operable without deep Kafka administration expertise for a portfolio-grade
  project

Apache Kafka was the natural choice as the industry standard for event
streaming at financial institutions. The open question was **which deployment
mode to use**: the classic Zookeeper-coordinated cluster, or the newer
KRaft (Kafka Raft Metadata) mode introduced as production-ready in
Kafka 3.3.

---

## Decision

**Use Apache Kafka 3.7.0 in KRaft mode — Zookeeper is not deployed.**

---

## Rationale

### 1. Eliminates an unnecessary service dependency

Zookeeper in a traditional Kafka deployment requires:
- A separate container running the `zookeeper` image
- A distinct port (2181) and volume
- A startup ordering dependency that Kafka brokers must wait on
- Additional health-check logic

In a Docker Compose stack already managing 12 services with a ~9.5 GB peak
RAM budget, removing Zookeeper saves roughly 300–400 MB of heap and one full
service slot. KRaft bundles metadata management inside the broker process
itself — the controller role runs in the same JVM.

### 2. KRaft is production-ready as of Kafka 3.3+

Confluent and the Apache Kafka community declared KRaft production-stable in
Kafka 3.3 (released 2022). Kafka 3.7.0 used in FraudLens has over two years
of production hardening behind it. The Zookeeper deprecation roadmap targets
full removal in a future major release, meaning KRaft is the forward-compatible
choice for any new Kafka deployment.

### 3. Simpler operational model

With KRaft, cluster state is managed through an internal Raft log partitioned
across controller nodes. For a single-broker development cluster, the broker
acts as both broker and controller. There is no split-brain risk across two
separate quorum systems (Kafka + Zookeeper), no need to manage Zookeeper
session timeouts, and no zoo.cfg to maintain.

### 4. Acceptable trade-offs at this scale

KRaft's current limitation — no support for some advanced multi-cluster
federation features — is irrelevant for a single-broker, 4-topic deployment.
The FraudLens topic set (`raw_transactions`, `enriched_transactions`,
`fraud_alerts`, `dlq_transactions`) fits comfortably within one broker.

---

## Alternatives Considered

| Option | Reason Rejected |
|---|---|
| **Kafka + Zookeeper (classic)** | Adds a service, RAM, and startup ordering complexity for no functional gain at this scale |
| **Redpanda** | Kafka-compatible, lower resource use, but introduces a non-standard binary and diverges from the industry standard interview artifact |
| **RabbitMQ** | Not partition-native; weaker fit for ordered, replayable event streams required by Spark offset tracking |
| **Amazon MSK / Confluent Cloud** | Requires external credentials, eliminates full local reproducibility, adds cost |

---

## Consequences

**Positive:**
- Docker Compose has 12 services instead of 13; startup is simpler and faster
- Full Kafka API compatibility — producer and consumer code is identical to
  a Zookeeper-mode cluster
- Forward-compatible with Kafka's published deprecation roadmap

**Negative / Risks:**
- KRaft's tooling for manual metadata inspection (`kafka-metadata-shell.sh`)
  is less familiar than ZooKeeper CLI (`zkCli.sh`) — acceptable for a
  portfolio project
- If migrating to a multi-broker production cluster, KRaft controller quorum
  sizing needs explicit attention (minimum 3 controllers recommended)

---

## Implementation Notes

```yaml
# docker-compose.yml — relevant Kafka broker environment variables
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_NODE_ID: 1
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qg  # fixed for reproducible local dev
```

Topic creation is handled by `kafka/topics/create_topics.sh`, called by
`make setup`, with replication-factor 1 and 3 partitions for
`raw_transactions`.

---

*See also: [ADR-002](ADR-002-microbatch.md) — Spark micro-batch interval,
[ADR-003](ADR-003-postgres-olap.md) — PostgreSQL as OLAP store*
