"""Iter-10 cmv_drilldown.

Tweaks the original parser for the new mzfg stack format introduced
in this build: each stack line now ends with `; <count> <thread_tag>`
(e.g. `; 1 coordinator`) instead of just `; <count>`. The original
parser's rsplit-on-space picks up the thread tag instead of the count.
"""
import bisect
import re
import sys
import json
from collections import defaultdict
sys.path.insert(0, "/tmp/mv_seed_metrics")
from analyze_flame import extract_mzfg


THREAD_TAG = re.compile(r"^[A-Za-z0-9_:./<>=,\-]+$")


def parse_mzfg_v2(mzfg: str):
    lines = mzfg.split("\n")
    i = 0
    while i < len(lines) and lines[i].strip() != "":
        i += 1
    i += 1
    stacks = []
    symbols = defaultdict(list)
    last_addr = None
    for line in lines[i:]:
        if not line:
            last_addr = None
            continue
        if line.startswith(";"):
            sym = line.lstrip(";").rstrip(";").strip()
            if last_addr is not None and sym:
                symbols[last_addr].append(sym)
            continue
        # Stack line? Starts with 0x and contains many `;`.
        if line.startswith("0x") and ";" in line:
            parts = line.rstrip().split()
            # Forms:
            #   "0x...;0x...; 1 coordinator"            -> 3 tokens after split
            #   "0x...;0x...; 1"                        -> 2 tokens
            #   "0x...; 1 coordinator"                  -> 3 tokens
            #   addresses separated by ';' inside parts[0]
            # We detect the count as the first all-digit token.
            count = None
            addr_blob = parts[0]
            for j, t in enumerate(parts[1:], start=1):
                if t.isdigit():
                    count = int(t)
                    # Anything before this beyond parts[0] is extra address chunks
                    addr_blob = " ".join([parts[0]] + parts[1:j])
                    break
            if count is None:
                # Fall through to symbol parsing
                pass
            else:
                addrs = [a for a in addr_blob.split(";") if a]
                stacks.append((addrs, count))
                last_addr = None
                continue
        # Symbol entry: "0xADDR name;"
        m = re.match(r"^(0x[0-9a-fA-F]+)\s+(.+?);?$", line)
        if m:
            last_addr = m.group(1)
            symbols[last_addr].append(m.group(2).rstrip(";"))
            continue
    return stacks, dict(symbols)


def main():
    html = sys.argv[1] if len(sys.argv) > 1 else "/tmp/iter10/cmv_flame_after_iter10.html"
    mzfg = extract_mzfg(html)
    stacks, symbols = parse_mzfg_v2(mzfg)
    total_samples = sum(c for _, c in stacks)
    print(f"# stacks: {len(stacks)}, total samples: {total_samples}, symbols: {len(symbols)}")

    sym_addrs = sorted(int(a, 16) for a in symbols.keys())
    sym_by_int = {int(a, 16): symbols[a][0] for a in symbols}

    def resolve(addr_str: str) -> str:
        if not addr_str.startswith("0x"):
            return ""
        ip = int(addr_str, 16)
        if ip == 0:
            return ""
        idx = bisect.bisect_right(sym_addrs, ip) - 1
        if idx < 0:
            return ""
        base = sym_addrs[idx]
        if ip - base > 16 * 1024 * 1024:
            return ""
        return sym_by_int[base]

    def resolved(addrs):
        return [resolve(a) for a in addrs]

    cmv_stacks = []
    for addrs, c in stacks:
        rs = resolved(addrs)
        if any("create_materialized_view" in n for n in rs):
            cmv_stacks.append((rs, c))
    cmv_total = sum(c for _, c in cmv_stacks)
    print(f"# {len(cmv_stacks)} stacks containing `create_materialized_view`, total {cmv_total} samples")
    print(f"# {total_samples} total samples in profile  ({100*cmv_total/total_samples:.1f}%)")

    NOISE = re.compile(r"poll|drop_in_place|^tokio::|^futures|^core::|Pin<|Box<|alloc::|hyper|h2::|tonic|metrics|tracing|tracker|^std::|^perf_signal|thread_start|lang_start|backtrace|signal|libc|fmod|memcpy|memset|bcmp|::call(_once)?$|FnOnce|FnMut|::call_mut\b|pthread_|epoll_|__sched_|__clock|clock_gettime|read|write|writev|fsync|syscall")

    def deepest_meaningful(rs):
        for n in reversed(rs):
            if not n:
                continue
            nn = re.sub(r"\[[0-9a-f]+\]", "", n)
            if NOISE.search(nn):
                continue
            return nn
        return "<all noise>"

    leaf_buckets = defaultdict(int)
    for rs, c in cmv_stacks:
        leaf_buckets[deepest_meaningful(rs)] += c

    print("\nDeepest meaningful frame (closest to leaf) per CMV stack:")
    print(f"{'symbol':<140}{'samples':>10}{'%cmv':>8}")
    print("-" * 158)
    for name, c in sorted(leaf_buckets.items(), key=lambda kv: -kv[1])[:35]:
        if len(name) > 138:
            name = name[:135] + "..."
        print(f"{name:<140}{c:>10}{100*c/cmv_total:>7.2f}%")

    markers = [
        ("durable persist work", [
            "PersistHandle::commit", "Transaction::commit", "TableTransaction::commit",
            "MaelstromConsensus", "consensus_cas", "blob_set", "external_op",
            "PersistClient::compare_and_set",
        ]),
        ("ReadHolds::id_bundle (should be 0 post-iter9)", [
            "ReadHolds::id_bundle",
        ]),
        ("rust-typed snapshot cache (write)", [
            "apply_to_snapshot_cache", "apply_updates",
        ]),
        ("snapshot clone / Transaction::new", [
            "Transaction::new", "MemorySnapshot",
        ]),
        ("CatalogState apply / mutate", [
            "CatalogState::apply_updates", "CatalogState::insert_entry",
            "CatalogState::get_schema_mut", "imbl::ord", "imbl::nodes",
            "ord::map::Node", "drop_in_place::<mz_catalog",
        ]),
        ("validate_resource_limits (target of iter-10 fix)", [
            "validate_resource_limits",
        ]),
        ("user_secrets/user_roles iterator filters", [
            "user_secrets", "user_roles", "user_network_policies",
            "is_secret", "is_network_policy",
        ]),
        ("builtin_table_updates emission", [
            "builtin_table_updates",
        ]),
        ("plan / optimize / lower", [
            "plan_create_materialized_view", "::optimize", "::transform",
            "MaterializedView::optimize",
        ]),
        ("compute controller round-trip", [
            "ComputeController::install_compute_collections",
            "ComputeController::create_dataflow",
            "ComputeController::create_dataflow_inner",
            "create_dataflow",
            "ComputeController::dataflow",
        ]),
        ("storage controller round-trip", [
            "StorageController::create_collections",
            "StorageController::register_collection_dependencies",
            "StorageController::send",
        ]),
        ("session bookkeeping", [
            "session::Session",
            "ExecuteContext", "send_immediate_rows",
            "stage_execute", "sequence_inner",
        ]),
        ("ingress (sql parse/purify)", [
            "purify_create_materialized_view", "purify::",
            "parse_statements",
        ]),
    ]

    print("\nGrouped inclusive breakdown (a stack is counted at most once per group):")
    print(f"{'group':<55}{'samples':>10}{'%cmv':>8}")
    for label, subs in markers:
        s = 0
        for rs, c in cmv_stacks:
            for n in rs:
                if any(sub in (n or "") for sub in subs):
                    s += c
                    break
        if s:
            print(f"{label:<55}{s:>10}{100*s/cmv_total:>7.2f}%")


if __name__ == "__main__":
    main()
