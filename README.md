# SCPro DISA Orders ACK Pipeline

An Apache Flink streaming job that consumes HL7 ACK^O21^ACK messages from Kafka and writes parsed acknowledgment data to PostgreSQL. The job is packaged as a fat jar and designed to run on a Flink session cluster managed by the Flink Kubernetes Operator. CI builds the fat jar and publishes it as a standalone OCI artifact to GHCR. The FlinkSessionJob uses the jar directly from GitHub releases.

## Features
- Kafka -> Flink -> PostgreSQL streaming pipeline for HL7 ACK messages
- Parses MSA segment data (acknowledgment code, message control ID, text message)
- Externalized configuration via environment variables and/or command-line arguments
- Deployable via Flink Kubernetes Operator (FlinkSessionJob)
- GitHub Actions pipeline builds the JAR and publishes it as a GHCR OCI artifact; the runtime uses the official Flink image

## Build
Requirements:
- JDK 17
- Gradle Wrapper included

Build fat jar:
- ./gradlew clean shadowJar

Run locally (example):
- export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
- export KAFKA_TOPIC=orders-ack
- export KAFKA_GROUP_ID=flink-local
- export JDBC_URL=jdbc:postgresql://localhost:5432/hie_manager
- export JDBC_USER=hie_manager_user
- export JDBC_PASSWORD=password
- java -jar build/libs/zm-scpro-disa-orders-ack-pipeline-*-all.jar --kafka.security.protocol=SASL_PLAINTEXT --kafka.sasl.mechanism=SCRAM-SHA-256 --kafka.sasl.username=admin --kafka.sasl.password=secret

Note: When running locally outside a Flink cluster, you might also use the application plugin:
- ./gradlew run --args="--kafka.bootstrap.servers=localhost:9092 --kafka.topic=orders-ack"

## Message Format
The pipeline processes HL7 ACK^O21^ACK messages with the following structure:

**Input Message Example:**
```
MSH|^~\&|DISA*LAB|ZLR|SmartCare|80080021|20250813115725||ACK^O21^ACK|GB7QBLS2-3HB0DTY3ZBN|P^T|2.5||||NE|ZMB
MSA|AE|435279bb-2729-43b6-9e2e-c87e32ad6403|Invalid Order Number (ORC.2). Cannot process order. Please correct and re-submit Order|
```

**Parsed Data:**
- MSA.1 (Acknowledgment Code) → `code` field in database
- MSA.2 (Message Control ID) → `order_message_ref_id` field in database  
- MSA.3 (Text Message) → `message` field in database

**Target Database Table:** `crt.order_ack`
- `id` (bigint, PK, FK to data.message.id)
- `code` (varchar(50)) - acknowledgment code (AE, AA, etc.)
- `order_message_ref_id` (varchar(100)) - message control ID
- `message` (varchar(250)) - text message

## Configuration
Configuration can be provided via environment variables or command-line arguments. Precedence: CLI args override environment variables; environment variables override built-in defaults. Secrets should be provided via environment variables or Kubernetes secrets, not hard-coded.

Supported keys (CLI args use --key=value form):
- kafka.bootstrap.servers / KAFKA_BOOTSTRAP_SERVERS
- kafka.topic / KAFKA_TOPIC (default: orders-ack)
- kafka.group.id / KAFKA_GROUP_ID (default: orders-ack-consumer)
- kafka.security.protocol / KAFKA_SECURITY_PROTOCOL (e.g., SASL_PLAINTEXT or SASL_SSL)
- kafka.sasl.mechanism / KAFKA_SASL_MECHANISM (e.g., SCRAM-SHA-256)
- kafka.sasl.username / KAFKA_SASL_USERNAME
- kafka.sasl.password / KAFKA_SASL_PASSWORD
- jdbc.url / JDBC_URL (e.g., jdbc:postgresql://host:5432/db)
- jdbc.user / JDBC_USER
- jdbc.password / JDBC_PASSWORD

Example CLI args:
- --kafka.bootstrap.servers=broker1:9092,broker2:9092
- --kafka.topic=orders-ack
- --jdbc.url=jdbc:postgresql://db:5432/hie_manager

## Runtime Image
This project no longer ships an application-specific runtime image. The Flink pods use the official Flink image. The job JAR is fetched directly by Flink from a GitHub Release asset via HTTPS using the job.jarURI field. No podTemplate or initContainers are used.

## Kubernetes Deployment (FlinkSessionJob)
This repo provides k8s/fleet/flink-sessionjob.yaml with a FlinkSessionJob targeting an existing session cluster named session-cluster.

Important:
- Ensure a FlinkDeployment session cluster named session-cluster exists in your namespace.
- Provide connector jars (Kafka/JDBC) compatible with Flink 1.20 in the cluster lib/ if not bundled.
- Inject non-secret configuration via job arguments in the FlinkSessionJob spec. Secrets should be provided via cluster-level configuration or externalized mechanisms; FlinkSessionJob here does not use a podTemplate.

Example job arguments in FlinkSessionJob:
  job:
    arguments:
      - "--kafka.topic=orders-ack"
      - "--kafka.group.id=orders-ack-consumer"

## CI/CD
The GitHub Actions workflow .github/workflows/ci.yml builds the fat JAR and publishes it as a GitHub Release asset. The workflow also outputs a resolved manifest artifact where the FlinkSessionJob's jarURI is substituted with the release asset HTTPS URL. Note: If the repository is private, the Flink Operator must be configured to access the URL (e.g., via credentials or by making the release public).

## Security Notes
- Provide secrets (Kafka SASL password, JDBC password) via environment variables or Kubernetes secrets.
- The app logs redact secret values but will show non-sensitive settings for troubleshooting.

## License
Add your license information here.