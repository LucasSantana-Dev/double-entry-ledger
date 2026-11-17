# 0026. No public deployment; a cost map from measured footprint instead

- Status: accepted
- Date: 2026-11-16

## Context

ADR 0024 left a public link as a later decision. The project is for a résumé and for study, so it does not need to be exposed. What it can still show is how much running it would cost and how small it can get.

## Options

1. Deploy to a cloud with a public link (researched: AWS EC2 t4g.medium with k3s at about $31 a month, AWS credits of up to $200 for new accounts).
2. Do not deploy. Measure the stack's real footprint, cut it, and publish a cost map: the smallest machine that runs it and its monthly price on AWS and Azure, plus throughput per dollar.

## Evidence

Official prices, 2026-11-16, us-east-1 / East US:

- AWS EC2 on demand ([AWS price list](https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/)): t4g.small (2 GB) $0.0168/h, about $12.26 a month; t4g.medium (4 GB) $0.0336/h, about $24.53. In sa-east-1 they cost $19.56 and $39.13.
- Public IPv4 on AWS: $0.005/h, about $3.65 a month ([VPC pricing](https://aws.amazon.com/vpc/pricing/)).
- AWS Lightsail Linux with IPv4: 2 GB at $12 and 4 GB at $24 a month ([Lightsail pricing](https://aws.amazon.com/lightsail/pricing/)).
- Azure VMs, East US ([Azure Retail Prices API](https://prices.azure.com/api/retail/prices)): B2s $0.0416/h (about $30.37), B2als v2 $0.0376/h (about $27.45).
- Managed Kafka is the expensive part: MSK Serverless at $0.75 per cluster hour, about $547 a month ([MSK pricing](https://aws.amazon.com/msk/pricing/)); Azure Event Hubs supports the Kafka protocol only from the Standard tier ([Event Hubs for Kafka](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview)).
- The current Kubernetes limits add up to about 2.5 GB (application 1 GiB, Kafka 1 GiB, PostgreSQL 512 MiB, `deploy/k8s/base`).

## Decision

Option 2, decided by the repo owner. Phase 5 measures and reduces the footprint (ADRs 0027 to 0029) and writes the cost map. Nothing is deployed and no cloud account is created.

## Consequences

- No running cost and no exposed service.
- The cost figures are estimates from list prices and measured resource use, not from a bill.
- The kind workflow (ADR 0024) stays the proof that the manifests deploy.
