# Brazilian Java job postings, phase 4 keywords (2026-11-16)

Evidence for the phase 4 ADRs (observability and deployment). Reproduce with `python tools/market/gupy_phase4_keywords.py`.

## Method

Same source and filter as the phase 3 sample: the public search endpoint of portal.gupy.io, `api/job-search/jobs?jobName=java`, fetched on 2026-11-16, keeping the postings whose title contains the word "Java": 100 postings from 47 companies. Case-insensitive regular expression matches in title plus description (HTML stripped); the patterns are in the script.

## Results

| Group | Keyword | Share of the 100 postings |
|---|---|---|
| Containers | Docker | 50% |
| | Kubernetes | 40% |
| | OpenShift | 13% |
| | Helm | 0% |
| Cloud | AWS | 35% |
| | Azure | 21% |
| | GCP | 12% |
| | any of AWS, Azure, GCP | 47% |
| | EKS | 7% |
| | Lambda | 6% |
| | ECS or Fargate | 4% |
| Delivery | CI/CD or pipelines | 48% |
| | Jenkins | 28% |
| | Terraform | 6% |
| | GitHub Actions | 6% |
| Observability | observability or monitoring (any sense) | 46% |
| | Grafana | 10% |
| | tracing | 10% |
| | ELK or Elastic | 8% |
| | New Relic | 6% |
| | Prometheus | 5% |
| | Datadog | 5% |
| | OpenTelemetry | 4% |
| | Dynatrace | 3% |
| | Splunk | 1% |
| | Micrometer, Jaeger, Zipkin | 0% |

## Caveats

- n = 100 from one portal on one day. Counts of 0 to 5 are noise: they say a term is rare, not that it is absent from the market.
- A keyword match does not separate required skills from nice-to-haves.
- "observability or monitoring" matches any sense of the words, including infrastructure monitoring duties.
- "pipelines" also matches data pipelines, so the CI/CD row is an upper bound.
- Postings change daily; a rerun will give different numbers.
