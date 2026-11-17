---
status: done
date: 2026-11-16
phase: 5
tags: [footprint, memory, startup, aot-cache, cost, benchmark]
---

# Phase 5: footprint, startup and a cost map

## Goal

Find out what the ledger stack costs to run and make it as small as it can reasonably get, without deploying it (ADR 0026). Each change is measured before and after, and the end result is a cost map: the smallest machine that runs the whole stack, its monthly list price on AWS and Azure, and created transfers per second per dollar.

## Scope

1. Measurement tool under `tools/footprint/`: starts the compose stack (application, PostgreSQL, Kafka; the observability container excluded), runs a fixed load, and records per container the peak resident memory, the CPU used, the application startup time (process start to readiness) and the image size. Output is a JSON file and a table.
2. Baseline: run the tool on the current stack and store the result.
3. Memory (ADR 0027): tune the application JVM (heap, metaspace, garbage collector for a small heap), the Hikari pool, the Kafka broker heap and PostgreSQL memory settings until the stack fits in 2 GB in total. Apply the limits in `compose.yaml` and in `deploy/k8s/base`.
4. Startup (ADR 0028): JVM AOT cache created by a training run in the Dockerfile and used at runtime; trim the image where it costs nothing.
5. Throughput per dollar (ADR 0029): run the conditional-update cells at 0%, 30% and 90% hot with the application limited to 2 CPUs and the lean memory settings, on GitHub Actions; store the results under `docs/benchmarks/<date>/`.
6. Cost map: `docs/costs/<date>-cost-map.md` with the measured footprint, the candidate machines and their monthly list prices (sources in ADR 0026), transfers per second per dollar, and the limits of these estimates. README section and roadmap row.

Out of scope: deploying anywhere, creating cloud accounts, a GraalVM native image, Spring AOT processing, changing the concurrency strategies, other benchmark strategies or the full grid, managed Kafka or PostgreSQL.

## Acceptance criteria

1. The footprint tool runs with one command and writes the same fields for every run; the baseline and the final results are committed.
2. With per-container memory limits that add up to 2 GB or less, the smoke test (`deploy/smoke.sh`) and the tool's load run pass with no container killed for memory. If 2 GB cannot be reached, the report states the smallest total that passed and what used the memory.
3. Application startup with the AOT cache is measured against startup without it on the same image build, median of at least 5 starts each.
4. The benchmark cells of scope item 5 run on CI with the run id recorded, and the ledger checks after each cell pass, as in phase 2.
5. The cost map lists at least t4g.small, t4g.medium, Lightsail 2 GB and 4 GB, and Azure B2als v2, with the price source and date, the measured footprint, and transfers per second per dollar for the machines the stack fits.
6. The seven phase 1 invariants, the five phase 3 invariants, the phase 4 criteria and `FaultInjectionTest` stay green; `./mvnw verify` and the kind workflow pass.

## Decisions

| ADR | Decision |
|---|---|
| 0026 | No public deployment; measure the footprint and publish a cost map |
| 0027 | The whole stack (application, PostgreSQL, Kafka) in 2 GB |
| 0028 | JVM AOT cache from a training run in the image build |
| 0029 | Conditional-update cells on a simulated 2 vCPU machine, throughput per dollar |

## Done when

- Criteria 1 to 6 hold.
- The cost map and the README section are written and have passed the humanizer pass the repo owner asked for.
