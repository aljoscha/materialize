# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""One serial, read-only observer for the original CI140146 quickstart failure.

Run as the CI agent on the host, after builds. Never owns or stops the workload.
A dispatched request failure disables HTTP, since server work may outlive curl.
Connection refusal can retry because no server work was submitted.
"""

import datetime
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

PARENT = Path("/sys/fs/cgroup/docker.slice")
BUDGET_FILES = (
    "cpu.max",
    "cpu.stat",
    "cpu.pressure",
    "memory.max",
    "memory.current",
    "memory.peak",
    "memory.stat",
    "memory.events",
    "memory.pressure",
    "memory.swap.max",
    "memory.swap.current",
)
IDENTITY = {"--catalog-cluster-id=u1", "--catalog-replica-id=u1"}


def now():
    return datetime.datetime.now(datetime.UTC).isoformat()


def process(pid):
    p = Path(f"/proc/{pid}")
    try:
        stat = (p / "stat").read_text()
        fields = stat.rsplit(")", 1)[1].split()
        return dict(
            pid=int(pid),
            start=fields[19],
            stat=stat,
            status=(p / "status").read_text(),
            args=(p / "cmdline").read_text().split("\0"),
            cgroup=(p / "cgroup").read_text(),
        )
    except (OSError, ProcessLookupError):
        return None


def accounting():
    child = resource.getrusage(resource.RUSAGE_CHILDREN)
    return dict(
        observer=process(os.getpid()),
        child_cpu_user=child.ru_utime,
        child_cpu_system=child.ru_stime,
        child_maxrss_kib=child.ru_maxrss,
    )


def limit_output():
    resource.setrlimit(resource.RLIMIT_FSIZE, (64 * 1024**2, 64 * 1024**2))


def request(
    out,
    label,
    pid,
    address,
    endpoint,
    seconds,
    *,
    expected_start,
    container_id,
    payload=None,
):
    # One direct curl in the pinned fixture container reaches the replica's
    # Unix socket without host root access or another Python interpreter.
    # CLI rusage is recorded below. The transient curl runs in the fixture
    # cgroup, whose accounting is sampled, not in the observer's child tree.
    if address.startswith("/"):
        command = ["docker", "exec", container_id, "/usr/bin/curl"]
        connection = ["--unix-socket", address, "http://localhost" + endpoint]
    elif address.startswith("http://127.0.0.1:"):
        command = ["/usr/bin/curl"]
        connection = [address + endpoint]
    else:
        raise ValueError("expected the captured workload's local endpoint")
    if payload is not None:
        connection += [
            "-H",
            "Content-Type: application/json",
            "--data-binary",
            json.dumps(payload),
        ]
    record = dict(
        start=now(),
        address=address,
        endpoint=endpoint,
        before=accounting(),
        target_before=process(pid),
        expected_start=expected_start,
        container_id=container_id,
        payload=payload,
    )
    if (
        record["target_before"] is None
        or record["target_before"]["start"] != expected_start
    ):
        record["error"] = "target identity changed before dispatch"
        (out / f"{label}.json").write_text(json.dumps(record))
        return False
    with (
        (out / f"{label}.body").open("wb") as body,
        (out / f"{label}.stderr").open("wb") as err,
    ):
        try:
            result = subprocess.run(
                [
                    *command,
                    "-fsS",
                    "--max-time",
                    str(seconds),
                    "--max-filesize",
                    str(64 * 1024**2),
                    *connection,
                ],
                stdout=body,
                stderr=err,
                timeout=seconds + 2,
                preexec_fn=limit_output,
            )
            record["exit"] = result.returncode
        except (OSError, subprocess.TimeoutExpired) as error:
            record["error"] = str(error)
    record.update(end=now(), after=accounting(), target_after=process(pid))
    (out / f"{label}.json").write_text(json.dumps(record))
    if record.get("exit") == 7:
        return None  # No connection, hence no outstanding server work.
    return (
        record.get("exit") == 0
        and record["target_before"] is not None
        and record["target_after"] is not None
        and record["target_before"]["start"] == record["target_after"]["start"]
    )


def main(out):
    out.mkdir(parents=True, exist_ok=False)
    assert PARENT.is_dir(), "original CI parent cgroup is missing"
    early_metrics = os.environ.get("CI_M2_EARLY_METRICS", "1") == "1"
    (out / "host.json").write_text(
        json.dumps(
            dict(
                time=now(),
                boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                clock_ticks=os.sysconf("SC_CLK_TCK"),
                page_size=os.sysconf("SC_PAGE_SIZE"),
                effective_uid=os.geteuid(),
                observer_pid=os.getpid(),
                python_version=sys.version,
                early_metrics=early_metrics,
                discovery_delay_seconds=240,
                cpu_after_final_heap={"seconds": 5, "hz": 99},
            )
        )
    )
    stop = out.parent / "stop"
    target = None
    heaps = 0
    catalog_address = None
    container = None
    container_process = None
    requests_enabled = True
    next_metrics = 0
    deadline = time.monotonic() + 900
    reason = "observer_deadline"
    # Keep discovery and sampling out of the observed startup/stall window.
    # The overall observer deadline still includes this delay.
    discovery_at = time.monotonic() + 240
    while time.monotonic() < discovery_at and not stop.exists():
        time.sleep(1)
    with (out / "samples.jsonl").open("w") as log:
        while time.monotonic() < deadline and not stop.exists():
            if container is None:
                try:
                    discovery = subprocess.run(
                        ["docker", "inspect", "platform-checks-materialized-1"],
                        capture_output=True,
                        text=True,
                        timeout=3,
                    )
                    if discovery.returncode == 0:
                        candidate = json.loads(discovery.stdout)[0]
                        if candidate["State"]["Running"]:
                            container_process = process(candidate["State"]["Pid"])
                            ports = candidate["NetworkSettings"]["Ports"].get(
                                "6878/tcp"
                            )
                            if container_process and ports:
                                container = candidate
                                catalog_address = (
                                    "http://127.0.0.1:" + ports[0]["HostPort"]
                                )
                                (out / "container.json").write_text(
                                    json.dumps(
                                        dict(
                                            id=container["Id"],
                                            image=container["Image"],
                                            process=container_process,
                                            address=catalog_address,
                                        )
                                    )
                                )
                except (
                    OSError,
                    subprocess.TimeoutExpired,
                    ValueError,
                    KeyError,
                    IndexError,
                ) as error:
                    (out / "discovery-error.txt").write_text(str(error))
            if container_process:
                live_container = process(container_process["pid"])
                if (
                    not live_container
                    or live_container["start"] != container_process["start"]
                ):
                    reason = "container_exited"
                    break
            if target is None and container is not None:
                matches = []
                for p in Path("/proc").iterdir():
                    if p.name.isdigit():
                        candidate = process(p.name)
                        if (
                            candidate
                            and IDENTITY <= set(candidate["args"])
                            and candidate["cgroup"] == container_process["cgroup"]
                        ):
                            matches.append(candidate)
                if len(matches) > 1:
                    reason = "ambiguous_target"
                    break
                if matches:
                    target = matches[0]
            current = process(target["pid"]) if target else None
            if target and (not current or current["start"] != target["start"]):
                reason = "target_exited"
                break
            sample = dict(time=now(), target=current, **accounting())
            sample["host"] = {
                f: Path("/proc", f).read_text()
                for f in (
                    "meminfo",
                    "stat",
                    "pressure/cpu",
                    "pressure/memory",
                    "pressure/io",
                )
                if Path("/proc", f).exists()
            }
            sample["budget"] = {
                f: (PARENT / f).read_text()
                for f in BUDGET_FILES
                if (PARENT / f).exists()
            }
            if current:
                if "/docker.slice/" not in current["cgroup"]:
                    reason = "target_outside_original_parent"
                    break
                sample["threads"] = []
                try:
                    threads = list(Path(f"/proc/{current['pid']}/task").iterdir())
                except OSError:
                    threads = []
                for t in threads:
                    try:
                        sample["threads"].append(
                            dict(tid=t.name, stat=(t / "stat").read_text())
                        )
                    except OSError:
                        pass
            log.write(json.dumps(sample) + "\n")
            log.flush()
            if sum(p.stat().st_size for p in out.glob("*.body")) > 192 * 1024**2:
                requests_enabled = False
            if log.tell() > 32 * 1024**2:
                reason = "accounting_output_limit"
                break
            if current and requests_enabled and not stop.exists():
                address = next(
                    (
                        a.split("=", 1)[1]
                        for a in current["args"]
                        if a.startswith("--internal-http-listen-addr=")
                    ),
                    None,
                )
                if not address:
                    reason = "target_has_no_http_address"
                    break
                anon = next(
                    (
                        int(s.split()[1]) * 1024
                        for s in current["status"].splitlines()
                        if s.startswith("RssAnon:")
                    ),
                    None,
                )
                if (
                    heaps < 2
                    and anon is not None
                    and anon >= (4 if heaps == 0 else 8) * 1024**3
                ):
                    result = request(
                        out,
                        f"heap-{heaps + 1}",
                        current["pid"],
                        address,
                        "/heap",
                        15,
                        expected_start=target["start"],
                        container_id=container["Id"],
                    )
                    requests_enabled = result is not False
                    if result is not None:
                        heaps += 1
                    # CPU sampling suspends heap sampling. Capture it only after
                    # the final heap so both heap snapshots cover allocations.
                    if result is True and heaps == 2 and not stop.exists():
                        result = request(
                            out,
                            "cpu-after-heap-2",
                            current["pid"],
                            address,
                            "/cpu",
                            8,
                            expected_start=target["start"],
                            container_id=container["Id"],
                            payload={"seconds": 5, "hz": 99, "merge_threads": False},
                        )
                        requests_enabled = result is not False
                if (
                    requests_enabled
                    and not stop.exists()
                    and sum(p.stat().st_size for p in out.glob("*.body"))
                    <= 192 * 1024**2
                    and early_metrics
                    and time.monotonic() >= next_metrics
                ):
                    result = request(
                        out,
                        f"metrics-{time.time_ns()}",
                        current["pid"],
                        address,
                        "/metrics",
                        3,
                        expected_start=target["start"],
                        container_id=container["Id"],
                    )
                    requests_enabled = result is not False
                    next_metrics = time.monotonic() + 10
            time.sleep(5)
    if stop.exists():
        reason = "workload_owner_stopped_observer"
    (out / "outcome.json").write_text(
        json.dumps(
            dict(
                time=now(),
                reason=reason,
                target=target,
                heaps_attempted=heaps,
                requests_enabled=requests_enabled,
                **accounting(),
            )
        )
    )


if __name__ == "__main__":
    main(Path(sys.argv[1]).resolve())
