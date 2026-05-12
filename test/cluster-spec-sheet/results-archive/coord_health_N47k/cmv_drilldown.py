"""For each stack containing `create_materialized_view`, walk from the
leaf (addrs[0]) upward and find the deepest meaningful Rust frame.
Aggregate by that frame to get a clearer breakdown of where CREATE
MATERIALIZED VIEW time goes.

Uses address-range lookup so samples that fall between symbol entries
(common in libc) still resolve to their containing function.
"""
import bisect
import re
import sys
from collections import defaultdict
sys.path.insert(0, "/tmp/mv_seed_metrics")
from analyze_flame import extract_mzfg, parse_mzfg


def main():
    html = sys.argv[1] if len(sys.argv) > 1 else "/tmp/mv_seed_metrics/coord_flame.html"
    mzfg = extract_mzfg(html)
    stacks, symbols = parse_mzfg(mzfg)

    # Address-range lookup: sort symbol addresses, find largest <= ip
    sym_addrs = sorted(int(a, 16) for a in symbols.keys())
    sym_by_int = {int(a, 16): symbols[a][0] for a in symbols}

    def resolve(addr_str: str) -> str:
        if not addr_str.startswith("0x"):
            return ""
        ip = int(addr_str, 16)
        if ip == 0:
            return ""
        i = bisect.bisect_right(sym_addrs, ip) - 1
        if i < 0:
            return ""
        base = sym_addrs[i]
        # Only accept if ip is within 16MB of base — heuristic to avoid
        # matching across unrelated regions
        if ip - base > 16 * 1024 * 1024:
            return ""
        return sym_by_int[base]

    # Build resolved stacks
    def resolved(addrs):
        out = []
        for a in addrs:
            r = resolve(a)
            out.append(r)
        return out

    # Filter to stacks containing create_materialized_view (substring)
    cmv_stacks = []
    for addrs, count in stacks:
        rs = resolved(addrs)
        if any("create_materialized_view" in n for n in rs):
            cmv_stacks.append((rs, count))

    cmv_total = sum(c for _, c in cmv_stacks)
    print(f"# {len(cmv_stacks)} stacks containing `create_materialized_view`, total {cmv_total} samples")
    print(f"# {sum(c for _, c in stacks)} total samples in profile")

    # For each cmv stack, find the deepest name (closest to leaf, addrs[0]) that:
    # (a) is resolved
    # (b) is not a pure runtime/poll/futures shim
    NOISE = re.compile(r"poll|drop_in_place|^tokio::|^futures|^core::|Pin<|Box<|alloc::|hyper|h2::|tonic|metrics|tracing|tracker|^std::|^perf_signal|thread_start|lang_start|backtrace|signal|libc|fmod|memcpy|memset|bcmp|::call(_once)?$|FnOnce|FnMut|::call_mut\b|pthread_|epoll_|__sched_|__clock|clock_gettime|read|write|writev|fsync|syscall")

    def deepest_meaningful(rs):
        # addrs[-1] is the leaf (most recent IP). Walk from leaf back
        # toward root, skipping pure runtime/poll noise, until we find
        # a useful frame.
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
        # rs[0] is innermost leaf; walk forward to find first non-noise frame
        leaf_buckets[deepest_meaningful(rs)] += c

    print("\nDeepest meaningful frame (closest to leaf) per CMV stack:")
    print(f"{'symbol':<140}{'samples':>10}{'%cmv':>8}")
    print("-" * 158)
    for name, c in sorted(leaf_buckets.items(), key=lambda kv: -kv[1])[:35]:
        if len(name) > 138:
            name = name[:135] + "..."
        print(f"{name:<140}{c:>10}{100*c/cmv_total:>7.2f}%")

    # Also: per-stack "what is happening" — show distribution of
    # specific high-value markers (broader than full substring search)
    markers = [
        ("durable persist work", [
            "PersistHandle::commit", "Transaction::commit", "TableTransaction::commit",
            "MaelstromConsensus", "consensus_cas", "blob_set", "external_op",
            "PersistClient::compare_and_set",
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
        ("validate_resource_limits", [
            "validate_resource_limits",
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
    print(f"{'group':<40}{'samples':>10}{'%cmv':>8}")
    for label, subs in markers:
        s = 0
        for rs, c in cmv_stacks:
            for n in rs:
                if any(sub in (n or "") for sub in subs):
                    s += c
                    break
        if s:
            print(f"{label:<40}{s:>10}{100*s/cmv_total:>7.2f}%")


if __name__ == "__main__":
    main()
