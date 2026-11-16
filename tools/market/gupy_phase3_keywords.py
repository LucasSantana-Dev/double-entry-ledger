"""Reproduce the Gupy keyword counts behind the phase 3 ADRs (0016 to 0019).

Stdlib only. Usage: python tools/market/gupy_phase3_keywords.py
Fetches every Gupy posting for the term "java", keeps those whose title contains the word "Java"
(case-insensitive) and counts case-insensitive keyword matches in title plus description.
The numbers drift as postings open and close; docs/market/2026-11-16-gupy-phase-3-keywords.md
records the 2026-11-16 run.
"""

import html
import json
import re
import urllib.request

API = "https://portal.gupy.io/api/job-search/jobs?jobName=java&limit=100&offset={offset}"

KEYWORDS = {
    "Kafka": r"kafka",
    "Spring Kafka": r"spring[\s-]*kafka",
    "RabbitMQ": r"rabbit",
    "SQS/SNS": r"\bsqs\b|\bsns\b",
    "Debezium": r"debezium",
    "CDC": r"\bcdc\b|change data capture",
    "outbox": r"outbox|out[\s-]box",
    "event-driven": r"event[\s-]*driven|orientad[ao]s? a eventos|arquitetura de eventos|baseada em eventos",
    "Avro": r"\bavro\b",
    "Protobuf": r"protobuf|protocol buffers",
    "Schema Registry": r"schema[\s-]*registry",
    "idempotency": r"idempot",
    "saga": r"\bsagas?\b",
    "Testcontainers": r"testcontainers?",
    "Toxiproxy": r"toxiproxy",
    "chaos": r"\bchaos\b",
    "resilience (any sense)": r"resili[eê]ncia|resilience|resilient|resiliente",
    "Resilience4j": r"resilience4j",
    "ShedLock": r"shedlock",
    "Spring Modulith": r"modulith",
    "OpenTelemetry": r"opentelemetry|open telemetry|\botel\b",
    "Prometheus": r"prometheus",
    "Grafana": r"grafana",
    "any of OpenTelemetry, Prometheus, Grafana": r"opentelemetry|open telemetry|\botel\b|prometheus|grafana",
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
