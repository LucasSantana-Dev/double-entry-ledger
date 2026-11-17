# Admission control tasks

- [x] Spec from the phase 6 review (2026-11-17)
- [x] Instrumentation: per-second pool and outcome series, PostgreSQL checkpoint and autovacuum logs, GC log
- [x] Limiter filter, off by default, with its tests; `hikari` arm as configuration
- [x] Cells and arms in the breaking-point run, one repetition per job, per-cell step length for the continuous hold
- [x] Workflow `admission-control.yml` and aggregator `tools/benchmark/admission.py`
- [x] Adversarial review of the experiment design; findings applied before the run
- [ ] Run on GitHub Actions, 5 jobs
- [ ] Results in `docs/benchmarks/<date>-admission-control/`
- [ ] ADR 0033 with the owner's choice; `docs/results.md` and README updated; spec status done
