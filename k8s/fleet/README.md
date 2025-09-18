# Kubernetes Deployment

This directory contains Kubernetes manifests for deploying the Orders ACK Pipeline to a Flink cluster.

## Files

- `flink-sessionjob.yaml`: FlinkSessionJob that references the session cluster and specifies the JAR URL
- `fleet.yaml`: Fleet GitOps configuration for automated deployment

## FlinkSessionJob

The FlinkSessionJob assumes:
- A FlinkDeployment session cluster named `session-cluster` exists in the `flink-jobs` namespace
- The cluster has Kafka and JDBC connectors compatible with Flink 1.20 available in lib/
- The JAR is published as a GitHub Release asset and accessible via HTTPS

## Configuration

Non-sensitive configuration is provided via job arguments in the FlinkSessionJob spec. 
Secrets should be provided via cluster-level configuration or environment variables.

Example job arguments:
```yaml
args:
  - "--kafka.topic=lab-orders-ack"
  - "--kafka.group.id=orders-ack-consumer"
  - "--jdbc.url=jdbc:postgresql://db:5432/hie_manager"
```