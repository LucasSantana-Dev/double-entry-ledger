"""Reproduce the Brazilian Java job-posting sample behind ADRs 0001 to 0004.

Stdlib only. Usage: python tools/market/gupy_java_sample.py
Counts keyword mentions in every Gupy posting whose title contains the word "Java".
The numbers drift as postings open and close; docs/market/ records the 2026-11-13 run.
"""

import html
import json
import re
import urllib.request

API = "https://portal.gupy.io/api/job-search/jobs?jobName={term}&limit=100&offset={offset}"

KEYWORDS = {
    "persistence": {
        "JPA/Hibernate/Spring Data": r"\bjpa\b|hibernate|spring\s*data",
        "jOOQ": r"\bjooq\b",
        "JDBC": r"\bjdbc",
        "MyBatis": r"mybatis",
    },
    "build": {"Maven": r"\bmaven\b", "Gradle": r"\bgradle\b"},
    "java version": {
        "Java 8": r"java\s*(se\s*)?8\b|java\s*1\.8",
        "Java 11": r"java\s*11\b",
        "Java 17": r"java\s*17\b",
        "Java 21": r"java\s*21\b",
        "Java 25": r"java\s*25\b",
    },
    "framework": {"Spring": r"\bspring\b", "Spring Boot": r"spring\s*boot", "Quarkus": r"quarkus"},
    "messaging": {"Kafka": r"kafka", "RabbitMQ": r"rabbit", "SQS": r"\bsqs\b"},
    "testing": {"JUnit": r"junit", "TDD": r"\btdd\b", "Testcontainers": r"testcontainers"},
}

# Benefit phrases that would otherwise match the finance regex ("banco de dados" is "database").
NOISE = r"bancos? de (dados|horas|talentos)|seguros? (de vida|sa[uú]de|odontol)|plano de sa[uú]de"
FINANCE = (r"\bbanc[oá]|banc[aá]ri|financeir|pagamento|\bpix\b|open\s*finance|cr[eé]dito|seguradora"
           r"|\bseguros\b|fintech|cooperativa|adquir[eê]ncia|consignado|investiment|c[aâ]mbio")


def fetch(term):
    jobs, offset = {}, 0
    while True:
        request = urllib.request.Request(API.format(term=term, offset=offset), headers={"User-Agent": "research-script"})
        with urllib.request.urlopen(request, timeout=60) as response:
            page = json.load(response)
        for job in page["data"]:
            jobs[job["id"]] = job
        offset += 100
        if offset >= page["pagination"]["total"]:
            return list(jobs.values())


def text(job):
    description = html.unescape(re.sub(r"<[^>]+>", " ", job.get("description") or ""))
    return f"{job['name']} {job.get('careerPageName') or ''} {description}".lower()


def main():
    jobs = [j for j in fetch("java") if re.search(r"\bjava\b", j["name"], re.I)]
    texts = [text(j) for j in jobs]
    n = len(jobs)
    print(f"{n} postings, {len({j.get('careerPageName') for j in jobs})} companies")
    for group, keywords in KEYWORDS.items():
        print(f"\n[{group}]")
        for label, pattern in keywords.items():
            hits = sum(1 for t in texts if re.search(pattern, t))
            print(f"  {label:<28}{hits:>4}/{n}  {100 * hits / n:5.1f}%")
    floor = sum(1 for j in jobs if re.search(FINANCE, f"{j['name']} {j.get('careerPageName') or ''}".lower()))
    ceiling = sum(1 for t in texts if re.search(FINANCE, re.sub(NOISE, " ", t)))
    print(f"\n[finance] title or company: {100 * floor / n:.1f}%, any mention without benefit noise: {100 * ceiling / n:.1f}%")


if __name__ == "__main__":
    main()
