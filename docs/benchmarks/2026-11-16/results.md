# Benchmark results

Run `43990396247` started 2026-11-17T00:47:24.728744398Z on AMD EPYC 9V45 96-Core Processor (4 logical CPUs), Linux 7.0.0-1019-azure, Java 25.0.5+8-LTS. Warm-up PT30S, measured PT1M, 5 repetitions in shuffled order. Application pinned to CPUs `0,1`, JVM options `-XX:+UseSerialGC -Xmx460m -XX:MaxMetaspaceSize=192m`.

Each value is the median over the repetitions, with its 95% bootstrap interval in brackets. Latency is for created transfers (201); `all p99` includes 503 answers (ADR 0015).

## Strategy grid (256 users, virtual threads)

| strategy | hot | users | threads | created/s | p50 ms | p99 ms | all p99 ms | retries | 503 | deadlocks | platform threads | heap MB | app CPU |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| conditional-update | 0% | 256 | virtual | 1,861 [1,802, 1,904] | 134 [132, 137] | 420 [398, 444] | 420 [398, 444] | 0 | 0 | 0 | 26 | 181 [178, 188] | 0.49 |
| conditional-update | 30% | 256 | virtual | 1,384 [1,316, 1,388] | 171 [170, 184] | 475 [469, 500] | 475 [469, 500] | 0 | 0 | 0 | 26 | 169 [166, 178] | 0.44 [0.43, 0.44] |
| conditional-update | 90% | 256 | virtual | 699 [645, 706] | 299 [295, 356] | 889 [764, 943] | 889 [764, 943] | 0 | 0 | 0 | 26 | 155 [155, 159] | 0.31 |

App CPU is the mean share of all logical CPUs used by the application process.
