"""Footprint of the ledger stack (phase 5, ADR 0027): memory, CPU, startup and image size.

Stdlib only. One command, from the repository root:

    python tools/footprint/footprint.py --label baseline

It starts compose.yaml plus tools/footprint/compose.footprint.yaml (application, PostgreSQL and
Kafka; no observability container) under its own project name, waits for the application's
readiness, runs deploy/smoke.sh, a fixed burst of concurrent transfers and deploy/smoke.sh again,
waits for the outbox and the statement to catch up, checks the ledger in the database, and
samples `docker stats` the whole time. Then it can restart the application N times to time its
startup (--startups). It writes docs/footprint/<date>-<label>.json and .md and removes the stack
with its volumes (unless --keep).

Fields per container: peak memory as `docker stats` reports it (usage without the inactive page
cache, what a memory limit is compared with), the cgroup's own memory.peak (page cache included),
the memory limit, peak and mean CPU percent (100 = one core), CPU seconds used during the load,
and whether the container was killed for memory. Application startup is the time from the
container's start to the first 200 of /actuator/health/readiness, measured by polling every
50 ms, next to Spring's own "process running for" figure from the log.
"""

import argparse
import concurrent.futures
import datetime
import json
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OVERRIDE = Path("tools/footprint/compose.footprint.yaml")
SERVICES = ["ledger", "postgres", "kafka"]
SEED = 20261116
UNITS = {"B": 1 / 2**20, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "kB": 1e3 / 2**20, "MB": 1e6 / 2**20, "GB": 1e9 / 2**20}


def run(cmd, check=True, capture=True, env=None):
    result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=capture, env=env)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed with {result.returncode}:\n{result.stdout}\n{result.stderr}")
    return result


class Stack:
    def __init__(self, project, port, extra_env):
        self.project = project
        self.port = port
        self.files = ["-f", "compose.yaml", "-f", str(OVERRIDE).replace("\\", "/")]
        if extra_env:
            # A second override with extra application environment, for example JVM options.
            handle, path = tempfile.mkstemp(prefix="footprint-", suffix=".yaml")
            with os.fdopen(handle, "w", encoding="utf-8") as f:
                f.write("services:\n  ledger:\n    environment:\n")
                for key, value in extra_env.items():
                    f.write(f"      {key}: {json.dumps(value)}\n")
            self.files += ["-f", path]
        self.env = dict(os.environ, FOOTPRINT_PORT=str(port))

    def compose(self, *args, check=True, capture=True):
        return run(["docker", "compose", "-p", self.project, *self.files, *args], check=check, capture=capture, env=self.env)

    def container(self, service):
        return self.compose("ps", "-a", "-q", service).stdout.strip()

    def sql(self, query):
        out = self.compose("exec", "-T", "postgres", "psql", "-U", "ledger", "-d", "ledger", "-tAc", query).stdout
        return out.strip()

    def sql_cmd(self):
        files = " ".join(f.replace("\\", "/") for f in self.files)
        return f"docker compose -p {self.project} {files} exec -T postgres psql -U ledger -d ledger -tAc"


def inspect(container):
    return json.loads(run(["docker", "inspect", container]).stdout)[0]


def parse_time(value):
    # Docker gives nanoseconds; Python takes microseconds.
    value = re.sub(r"(\.\d{6})\d+", r"\1", value).replace("Z", "+00:00")
    return datetime.datetime.fromisoformat(value)


def mib(text):
    m = re.match(r"([\d.]+)\s*([A-Za-z]+)", text.strip())
    return float(m.group(1)) * UNITS[m.group(2)] if m else 0.0


class Sampler(threading.Thread):
    """`docker stats --no-stream` in a loop; each call takes about a second."""

    def __init__(self, names):
        super().__init__(daemon=True)
        self.names = names
        self.samples = {name: [] for name in names}
        self.stopping = threading.Event()

    def run(self):
        while not self.stopping.is_set():
            out = run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *self.names], check=False).stdout
            for line in out.splitlines():
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                name = row.get("Name")
                if name in self.samples:
                    used = row["MemUsage"].split("/")[0]
                    self.samples[name].append((mib(used), float(row["CPUPerc"].rstrip("%") or 0)))

    def stop(self):
        self.stopping.set()
        self.join(timeout=30)


def cgroup(container, file):
    out = run(["docker", "exec", container, "cat", f"/sys/fs/cgroup/{file}"], check=False).stdout
    return out


def cpu_seconds(container):
    m = re.search(r"usage_usec (\d+)", cgroup(container, "cpu.stat"))
    return int(m.group(1)) / 1e6 if m else None


def memory_peak_mib(container):
    out = cgroup(container, "memory.peak").strip()
    return round(int(out) / 2**20, 1) if out.isdigit() else None


def http(method, url, key=None, body=None, timeout=30):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method)
    if body is not None:
        request.add_header("Content-Type", "application/json")
    if key:
        request.add_header("Idempotency-Key", key)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
        return 0, str(e)


def wait_ready(base, started_at, timeout=180):
    """Seconds from the container start to the first ready answer, by the host clock."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, _ = http("GET", f"{base}/actuator/health/readiness", timeout=2)
        if status == 200:
            now = datetime.datetime.now(datetime.timezone.utc)
            return round((now - started_at).total_seconds(), 2)
        time.sleep(0.05)
    raise RuntimeError(f"not ready after {timeout}s")


def spring_started(container, since):
    """Spring's 'process running for X' from the last start's log."""
    logs = run(["docker", "logs", "--since", since, container], check=False)
    found = re.findall(r"process running for ([\d.]+)", logs.stdout + logs.stderr)
    return float(found[-1]) if found else None


def start_ledger_timing(stack, base):
    container = stack.container("ledger")
    state = inspect(container)["State"]
    started = parse_time(state["StartedAt"])
    ready = wait_ready(base, started)
    return {"readinessSeconds": ready, "springProcessRunningSeconds": spring_started(container, state["StartedAt"])}


def smoke(stack, base):
    bash = shutil.which("bash")
    if not bash:
        return {"passed": False, "output": "bash not found"}
    env = dict(os.environ, BASE_URL=base, SQL_CMD=stack.sql_cmd())
    result = subprocess.run([bash, "deploy/smoke.sh"], cwd=ROOT, env=env, text=True, capture_output=True)
    return {"passed": result.returncode == 0, "output": (result.stdout + result.stderr).strip().splitlines()[-1:]}


def load(base, accounts, transfers, concurrency, hot_share):
    """Opens and funds accounts, then sends transfers from `concurrency` threads; 30% touch account 0."""
    run_id = f"fp-{int(time.time())}"
    ids = []
    for i in range(accounts):
        status, body = http("POST", f"{base}/accounts", key=f"{run_id}-acc-{i}")
        if status != 201:
            raise RuntimeError(f"account {i}: {status} {body}")
        ids.append(json.loads(body)["id"])
        status, body = http("POST", f"{base}/deposits", key=f"{run_id}-dep-{i}",
                            body={"accountId": ids[-1], "amount": "1000.00"})
        if status != 201:
            raise RuntimeError(f"deposit {i}: {status} {body}")
    rng = random.Random(SEED)
    plan = []
    for n in range(transfers):
        a, b = rng.sample(range(accounts), 2)
        if rng.random() < hot_share:
            a, b = (0, b if b != 0 else 1) if rng.random() < 0.5 else (a if a != 0 else 1, 0)
        plan.append((n, ids[a], ids[b], f"{rng.randint(1, 100) / 100:.2f}"))
    statuses = {}
    lock = threading.Lock()

    def send(item):
        n, a, b, amount = item
        status, _ = http("POST", f"{base}/transfers", key=f"{run_id}-t-{n}",
                         body={"fromAccountId": a, "toAccountId": b, "amount": amount})
        with lock:
            statuses[status] = statuses.get(status, 0) + 1

    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(send, plan))
    seconds = time.time() - start
    return {"accounts": accounts, "transfers": transfers, "concurrency": concurrency, "hotShare": hot_share,
            "seconds": round(seconds, 1), "createdPerSecond": round(statuses.get(201, 0) / seconds, 1),
            "statuses": {str(k): v for k, v in sorted(statuses.items())}}


def wait_drained(stack, timeout=180):
    """Outbox empty and every event applied to the statement."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        pending = int(stack.sql("select count(*) from outbox_events where published_at is null and parked_at is null") or -1)
        events = int(stack.sql("select count(*) from outbox_events") or -1)
        applied = int(stack.sql("select count(*) from statement.processed_events") or -1)
        if pending == 0 and applied >= events:
            return {"drained": True, "events": events, "applied": applied}
        time.sleep(2)
    return {"drained": False, "events": events, "applied": applied, "pending": pending}


def ledger_checks(stack):
    return {
        "sumOfBalancesCents": int(stack.sql("select coalesce(sum(balance_cents), 0) from accounts")),
        "negativeCustomerBalances": int(stack.sql("select count(*) from accounts where kind = 'CUSTOMER' and balance_cents < 0")),
        "balancesDifferentFromEntries": int(stack.sql(
            "select count(*) from accounts a where a.balance_cents <> "
            "coalesce((select sum(e.amount_cents) from entries e where e.account_id = a.id), 0)")),
    }


def image_info(image):
    data = json.loads(run(["docker", "image", "inspect", image]).stdout)[0]
    return {"image": image, "id": data["Id"][:19], "sizeMB": round(data["Size"] / 1e6, 1)}


def app_files(image):
    """Size of /app in the application image and of the AOT cache in it, if any."""
    out = run(["docker", "run", "--rm", "--entrypoint", "sh", image, "-c",
               "du -sk /app; ls -l /app/*.aot 2>/dev/null || true"], check=False).stdout
    m = re.search(r"^(\d+)\s+/app", out, re.M)
    aot = re.search(r"\s(\d+)\s+\w+\s+\d+\s+[\d:]+\s+(\S+\.aot)", out)
    return {"appDirMB": round(int(m.group(1)) / 1024, 1) if m else None,
            "aotCacheMB": round(int(aot.group(1)) / 2**20, 1) if aot else None}


def summarize_container(name, samples, info, cpu_used):
    mem = [s[0] for s in samples]
    cpu = [s[1] for s in samples]
    config = info["Config"]
    env = dict(e.split("=", 1) for e in config.get("Env") or [] if "=" in e)
    limit = info["HostConfig"].get("Memory") or 0
    return {
        "image": config["Image"],
        "peakMemMiB": round(max(mem), 1) if mem else None,
        "meanMemMiB": round(statistics.mean(mem), 1) if mem else None,
        "cgroupPeakMiB": None,  # filled in before the stack goes down
        "memLimitMiB": round(limit / 2**20) if limit else None,
        "cpuLimit": (info["HostConfig"].get("NanoCpus") or 0) / 1e9 or None,
        "peakCpuPercent": round(max(cpu), 1) if cpu else None,
        "meanCpuPercent": round(statistics.mean(cpu), 1) if cpu else None,
        "loadCpuSeconds": cpu_used,
        "samples": len(samples),
        "oomKilled": info["State"]["OOMKilled"],
        "status": info["State"]["Status"],
        "exitCode": info["State"]["ExitCode"],
        "restartCount": info.get("RestartCount", 0),
        "settings": {k: env[k] for k in ("JAVA_TOOL_OPTIONS", "KAFKA_HEAP_OPTS", "SPRING_DATASOURCE_HIKARI_MAXIMUMPOOLSIZE") if k in env}
                    | ({"command": " ".join(config.get("Cmd") or [])} if name == "postgres" else {}),
    }


def markdown(result):
    lines = [
        f"# Footprint: {result['label']}",
        "",
        f"Run {result['startedAt']} on {result['host']['os']}, Docker {result['host']['docker']}, "
        f"{result['host']['cpus']} CPUs and {result['host']['memGiB']} GiB for Docker. "
        f"Commit `{result['commit']}`. Tool: `python tools/footprint/footprint.py {' '.join(result['args'])}`.",
        "",
        "| container | peak MiB | mean MiB | cgroup peak MiB | limit MiB | peak CPU % | mean CPU % | CPU s in load | OOM killed | exit |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    total_peak = 0
    total_limit = 0
    for name, c in result["containers"].items():
        total_peak += c["peakMemMiB"] or 0
        total_limit += c["memLimitMiB"] or 0
        lines.append(f"| {name} | {c['peakMemMiB']} | {c['meanMemMiB']} | {c['cgroupPeakMiB']} | {c['memLimitMiB'] or 'none'} | "
                     f"{c['peakCpuPercent']} | {c['meanCpuPercent']} | {c['loadCpuSeconds']} | {c['oomKilled']} | {c['exitCode']} |")
    lines.append(f"| **total** | **{round(total_peak, 1)}** | | | **{total_limit or 'none'}** | | | | | |")
    lines += ["", "Peak and mean MiB are `docker stats` samples (page cache not counted) over the whole run; "
              "cgroup peak is the kernel's memory.peak, page cache included, since the container's last (re)start. "
              "CPU % is per core (100 = one core).", "", "Settings in effect:", ""]
    for name, c in result["containers"].items():
        for key, value in c["settings"].items():
            lines.append(f"- {name} `{key}`: `{value}`")
    s = result["startup"]
    lines += ["", f"Application startup (container start to readiness): {s['first']['readinessSeconds']} s; "
              f"Spring's 'process running for': {s['first']['springProcessRunningSeconds']} s."]
    if s.get("restarts"):
        lines.append(f"Restarts: {len(s['restarts'])}, median readiness {s['medianReadinessSeconds']} s, "
                     f"median Spring {s['medianSpringSeconds']} s, readiness values {[r['readinessSeconds'] for r in s['restarts']]}.")
    img = result["images"]["ledger"]
    lines += ["", f"Application image `{img['image']}`: {img['sizeMB']} MB on disk (uncompressed), "
              f"/app {img.get('appDirMB')} MB, AOT cache {img.get('aotCacheMB')} MB."]
    if result.get("load"):
        ld = result["load"]
        lines += ["", f"Load: smoke before {result['smokeBefore']['passed']}, "
                  f"{ld['transfers']} transfers over {ld['accounts']} accounts from {ld['concurrency']} threads "
                  f"({ld['hotShare']:.0%} hot) in {ld['seconds']} s, statuses {ld['statuses']}, "
                  f"smoke after {result['smokeAfter']['passed']}, outbox drained {result['drain']['drained']} "
                  f"({result['drain']['events']} events), ledger checks {result['ledgerChecks']}."]
    lines += ["", f"Passed: **{result['passed']}**", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--label", required=True)
    parser.add_argument("--project", default="ledger-footprint")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--accounts", type=int, default=50)
    parser.add_argument("--transfers", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--hot-share", type=float, default=0.3)
    parser.add_argument("--startups", type=int, default=0, help="restart the application N times and time each start")
    parser.add_argument("--skip-load", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--keep", action="store_true", help="leave the stack running")
    parser.add_argument("--ledger-env", action="append", default=[], metavar="KEY=VALUE",
                        help="extra application environment, for example JAVA_TOOL_OPTIONS=...")
    parser.add_argument("--out", default="docs/footprint")
    args = parser.parse_args()

    extra_env = dict(item.split("=", 1) for item in args.ledger_env)
    stack = Stack(args.project, args.port, extra_env)
    base = f"http://localhost:{args.port}"
    started_at = datetime.datetime.now(datetime.timezone.utc)
    result = {"label": args.label, "startedAt": started_at.isoformat(timespec="seconds"), "args": sys.argv[1:],
              "commit": run(["git", "rev-parse", "--short", "HEAD"]).stdout.strip()
              + ("-dirty" if run(["git", "status", "--porcelain", "--untracked-files=no"]).stdout.strip() else "")}
    info = json.loads(run(["docker", "info", "--format", "{{json .}}"]).stdout)
    result["host"] = {"os": info.get("OperatingSystem"), "kernel": info.get("KernelVersion"), "docker": info.get("ServerVersion"),
                      "cpus": info.get("NCPU"), "memGiB": round(info.get("MemTotal", 0) / 2**30, 1)}

    stack.compose("down", "-v", check=False)
    if not args.no_build:
        stack.compose("build", "ledger", capture=False)
    stack.compose("create", *SERVICES)
    names = {s: inspect(stack.container(s))["Name"].lstrip("/") for s in SERVICES}
    sampler = Sampler(list(names.values()))
    sampler.start()
    passed = True
    before, after = {}, {}
    try:
        stack.compose("up", "-d", *SERVICES)
        result["startup"] = {"first": start_ledger_timing(stack, base)}
        if not args.skip_load:
            result["smokeBefore"] = smoke(stack, base)
            before = {s: cpu_seconds(names[s]) for s in SERVICES}
            result["load"] = load(base, args.accounts, args.transfers, args.concurrency, args.hot_share)
            after = {s: cpu_seconds(names[s]) for s in SERVICES}
            result["smokeAfter"] = smoke(stack, base)
            result["drain"] = wait_drained(stack)
            result["ledgerChecks"] = ledger_checks(stack)
            passed = (result["smokeBefore"]["passed"] and result["smokeAfter"]["passed"] and result["drain"]["drained"]
                      and not any(result["ledgerChecks"].values()) and "0" not in result["load"]["statuses"])
        restarts = []
        for _ in range(args.startups):
            run(["docker", "restart", names["ledger"]])
            restarts.append(start_ledger_timing(stack, base))
        if restarts:
            result["startup"]["restarts"] = restarts
            result["startup"]["medianReadinessSeconds"] = statistics.median(r["readinessSeconds"] for r in restarts)
            spring = [r["springProcessRunningSeconds"] for r in restarts if r["springProcessRunningSeconds"]]
            result["startup"]["medianSpringSeconds"] = statistics.median(spring) if spring else None
    finally:
        sampler.stop()
        containers = {}
        for s in SERVICES:
            container_info = inspect(names[s])
            used = round(after[s] - before[s], 1) if before.get(s) is not None and after.get(s) is not None else None
            containers[s] = summarize_container(s, sampler.samples[names[s]], container_info, used)
            containers[s]["cgroupPeakMiB"] = memory_peak_mib(names[s])
            if container_info["State"]["OOMKilled"] or container_info["State"]["Status"] != "running":
                passed = False
        result["containers"] = containers
        result["totalPeakMemMiB"] = round(sum(c["peakMemMiB"] or 0 for c in containers.values()), 1)
        result["totalMemLimitMiB"] = sum(c["memLimitMiB"] or 0 for c in containers.values()) or None
        ledger_image = containers["ledger"]["image"]
        result["images"] = {s: image_info(containers[s]["image"]) for s in SERVICES}
        result["images"]["ledger"] |= app_files(ledger_image)
        result["passed"] = passed
        if not args.keep:
            stack.compose("down", "-v", check=False)

    out = ROOT / args.out
    out.mkdir(parents=True, exist_ok=True)
    stem = f"{started_at.astimezone().date().isoformat()}-{args.label}"
    (out / f"{stem}.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    (out / f"{stem}.md").write_text(markdown(result), encoding="utf-8")
    print(markdown(result))
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
