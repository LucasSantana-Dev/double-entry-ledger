# 0024. Deployment: Kubernetes manifests with Kustomize, deployed to kind in CI

- Status: accepted
- Date: 2026-11-16

## Context

Phase 4 ships the ledger somewhere it can be seen running. Brazilian Java postings ask for containers, Kubernetes and a cloud far more than for any single host, and the project budget is close to zero.

## Options

Target:

1. Kubernetes manifests deployed to a kind cluster in a GitHub Actions job with a smoke test; no public link for now.
2. k3s on a Hetzner VM with Terraform: public link, about 6 to 9 EUR a month plus operations.
3. k3s on Oracle Cloud Always Free: public link at no cost.
4. Managed serverless: Azure Container Apps, Confluent Cloud Basic, Aiven free PostgreSQL.

Packaging:

1. Plain YAML with Kustomize (base plus a kind overlay), PostgreSQL and a single-node KRaft Kafka as StatefulSets.
2. A Helm chart.
3. Kustomize for the application, Kafka through the Strimzi operator.

## Evidence

- Market (Gupy, 100 Java postings, 2026-11-16, [docs/market/2026-11-16-gupy-phase-4-keywords.md](../market/2026-11-16-gupy-phase-4-keywords.md)): Docker 50%, Kubernetes 40%, AWS 35%, Azure 21%, OpenShift 13%, GCP 12%, Terraform 6%, GitHub Actions 6%, Helm 0%.
- Costs and limits on the providers' pages, 2026-11-16:
  - AWS App Runner is closed to new customers ([AWS](https://docs.aws.amazon.com/apprunner/latest/dg/apprunner-availability-change.html)).
  - The Aiven free Kafka plan allows 2 partitions per topic, below the 3 of ADR 0016, and loses topic data when it powers off ([Aiven](https://aiven.io/docs/products/kafka/free-tier/kafka-free-tier)).
  - MSK Serverless costs $0.75 per cluster hour, about $548 a month ([AWS](https://aws.amazon.com/msk/pricing/)).
  - Oracle Always Free is now 2 OCPU and 12 GB, with idle reclamation ([Oracle](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm)).
  - Hetzner cloud servers start at 5.49 EUR a month ([Hetzner](https://www.hetzner.com/cloud/)).
  - Neon does not expose instance parameters on its plans below Scale ([Neon](https://neon.com/docs/reference/compatibility)).
- `kubectl apply -k` runs Kustomize without another tool. Strimzi needs its operator installed in the cluster before the Kafka resource, which adds time and parts to every CI run.

## Decision

Target option 1 and packaging option 1, decided by the repo owner.

- A multi-stage `Dockerfile` (JDK 25 build, JRE 25 runtime, non-root user).
- `deploy/k8s/base` with the application Deployment and Service, PostgreSQL 18 and Kafka (KRaft, one node) StatefulSets, the topic setup, configuration in a ConfigMap and credentials in a Secret; `deploy/k8s/overlays/kind` for the CI cluster.
- Liveness and readiness from the Actuator health groups.
- A GitHub Actions workflow builds the image, creates a kind cluster, loads the image, applies the overlay, waits for the rollout and runs a smoke test (an account, a deposit, a transfer, the statement line).

## Consequences

- No monthly cost and no servers to operate. Docker, Kubernetes and GitHub Actions are shown.
- No public link for now. The same base can take a cloud overlay later; that is a new decision.
- No hyperscaler (AWS, Azure) in the project yet, though together with GCP they appear in 47% of the postings.
- Single-node Kafka and PostgreSQL in the cluster are for the demo, not for availability.
