"""Reproduce the Gupy keyword counts behind the phase 4 ADRs (deployment and observability).

Stdlib only. Usage: python tools/market/gupy_phase4_keywords.py
Fetches every Gupy posting for the term "java", keeps those whose title contains the word "Java"
(case-insensitive) and counts case-insensitive keyword matches in title plus description.
The numbers drift as postings open and close; docs/market/2026-11-16-gupy-phase-4-keywords.md
records the 2026-11-16 run.
"""

import html
import json
import re
import urllib.request

API = "https://portal.gupy.io/api/job-search/jobs?jobName=java&limit=100&offset={offset}"

KEYWORDS = {
    "AWS": r"\baws\b|amazon web services",
    "Azure": r"\bazure\b",
    "GCP": r"\bgcp\b|google cloud",
    "any cloud (AWS, Azure, GCP)": r"\baws\b|amazon web services|\bazure\b|\bgcp\b|google cloud",
    "Docker": r"docker",
    "Kubernetes": r"kubernetes|\bk8s\b",
    "OpenShift": r"openshift",
    "Helm": r"\bhelm\b",
    "Terraform": r"terraform",
    "ECS/Fargate": r"\becs\b|fargate",
    "EKS": r"\beks\b",
    "Lambda": r"\blambda\b",
    "CI/CD": r"ci/cd|ci\s*/\s*cd|pipelines?",
    "GitHub Actions": r"github actions",
    "Jenkins": r"jenkins",
    "OpenTelemetry": r"opentelemetry|open telemetry|\botel\b",
    "Prometheus": r"prometheus",
    "Grafana": r"grafana",
    "Micrometer": r"micrometer",
    "Jaeger": r"jaeger",
    "Zipkin": r"zipkin",
    "Datadog": r"datadog",
    "New Relic": r"new\s*relic",
    "Dynatrace": r"dynatrace",
    "Splunk": r"splunk",
    "ELK/Elastic": r"\belk\b|elasticsearch|kibana|logstash",
    "observability (any sense)": r"observabilidade|observability|monitoramento|monitoring",
    "tracing": r"tracing|rastreamento distribu",
}


def fetch():
    jobs, offset = {}, 0
    while True:
        request = urllib.request.Request(API.format(offset=offset), headers={"User-Agent": "research-script"})
        with urllib.request.urlopen(request, timeout=60) as response:
            page = json.load(response)
        for job in page["data"]:
            jobs[job["id"]] = job
        offset += 100
        if offset >= page["pagination"]["total"]:
            return list(jobs.values())


def text(job):
    description = html.unescape(re.sub(r"<[^>]+>", " ", job.get("description") or ""))
    return f"{job['name']} {description}".lower()


def main():
    jobs = [job for job in fetch() if re.search(r"\bjava\b", job["name"], re.I)]
    texts = [text(job) for job in jobs]
    total = len(jobs)
    companies = len({job.get("careerPageName") for job in jobs})
    print(f"{total} postings from {companies} companies")
    print("keyword|pattern|postings|share")
    for name, pattern in KEYWORDS.items():
        hits = sum(1 for t in texts if re.search(pattern, t))
        print(f"{name}|{pattern}|{hits}|{100 * hits / total:.1f}%")


if __name__ == "__main__":
    main()
