"""Parse a Materialize CPU flamegraph (mzfg embedded in HTML) and report
where samples fall — aggregated per-symbol and per-symbol-substring.

The mzfg format is:

    !!! COMMENT !!!: Open with ...
    mz_fg_version: 1
    Sampling time (s): 30
    Sampling frequency (Hz): 99
    \n
    <stack> <count>\n        ← stacks section
    <stack> <count>\n
    ...
    \n
    0xADDR symbol_name;\n    ← symbols section (continuation lines have leading semicolon)
    ...

A stack is a semicolon-separated list of hex addresses (leaf to root or
root to leaf depending on the producer — for jemalloc CPU prof, it's
leaf-first). Multiple addresses can map to the same symbol via inlining.

We do two kinds of aggregation:
* "Self time" per symbol: just the leaf address of each stack.
* "Inclusive time" per symbol: any sample whose stack contains the
  symbol's address.

Usage:
    python3 analyze_flame.py /tmp/mv_seed_metrics/coord_flame.html
"""
import json
import re
import sys
from collections import defaultdict


def extract_mzfg(html_path: str) -> str:
    text = open(html_path).read()
    # Find: let mzfg = "..."
    m = re.search(r'let mzfg\s*=\s*"((?:[^"\\]|\\.)*)"', text)
    if not m:
        raise SystemExit("could not locate mzfg in HTML")
    raw = m.group(1)
    # Decode JS escapes
    return raw.encode().decode("unicode_escape")


def parse_mzfg(mzfg: str):
    """Return (stacks, symbols) where:
       stacks = [(list[address_hex_str], int_count), ...]
       symbols = {address_hex_str -> [list of symbol strings]}
    """
    lines = mzfg.split("\n")
    # Skip header until blank line
    i = 0
    while i < len(lines) and lines[i].strip() != "":
        i += 1
    i += 1  # skip blank line

    stacks = []
    symbols = defaultdict(list)
    last_addr = None

    while i < len(lines):
        line = lines[i]
        i += 1
        if not line:
            # End of stacks section, blank line → symbols start
            last_addr = None
            continue
        # Symbol-continuation lines start with ';' (additional inline)
        if line.startswith(";"):
            sym = line.lstrip(";").rstrip(";").strip()
            if last_addr is not None and sym:
                symbols[last_addr].append(sym)
            continue
        # A line like "0xADDR symbol_name;" → symbol entry
        m = re.match(r"^(0x[0-9a-fA-F]+)\s+(.+?);?$", line)
        if m and " " in line and not line.endswith(";"):
            # Stacks can also start with 0x... but they have a trailing " <count>"
            # disambiguate: stacks line ends with " <int>" (whitespace then int)
            tail = line.rsplit(" ", 1)
            if tail[-1].isdigit() and ";" in tail[0]:
                addrs = [a for a in tail[0].split(";") if a]
                stacks.append((addrs, int(tail[-1])))
                last_addr = None
                continue
            # Otherwise it's a symbol entry
            last_addr = m.group(1)
            symbols[last_addr].append(m.group(2).rstrip(";"))
            continue
        # Stack line: addresses + count separated by single space
        tail = line.rsplit(" ", 1)
        if len(tail) == 2 and tail[1].strip().isdigit() and ";" in tail[0]:
            addrs = [a for a in tail[0].split(";") if a]
            stacks.append((addrs, int(tail[1])))
            last_addr = None
            continue
        # Other: maybe a symbol line that ended with ; explicitly
        m = re.match(r"^(0x[0-9a-fA-F]+)\s+(.+);$", line)
        if m:
            last_addr = m.group(1)
            symbols[last_addr].append(m.group(2))
            continue

    return stacks, dict(symbols)


def main():
    html = sys.argv[1] if len(sys.argv) > 1 else "/tmp/mv_seed_metrics/coord_flame.html"
    mzfg = extract_mzfg(html)
    stacks, symbols = parse_mzfg(mzfg)
    total_samples = sum(c for _, c in stacks)
    print(f"# stacks: {len(stacks)}, total samples: {total_samples}, symbols: {len(symbols)}")

    # Build a lookup: address -> "first symbol" (the outermost name)
    addr_to_name = {a: (s[0] if s else "") for a, s in symbols.items()}

    # Inclusive samples per substring of interest
    substrings = [
        "create_materialized_view",
        "sequence_create_materialized_view",
        "ExecuteContext",
        "explain_plan",
        "lower",
        "Optimize",
        "optimize",
        "Resolver",
        "transform",
        "resolve",
        "purify",
        "plan_create_materialized_view",
        "Catalog::transact",
        "catalog_transact",
        "::Catalog>::sequence",
        "validate_resource_limits",
        "apply_updates",
        "apply_to_snapshot_cache",
        "Transaction::commit",
        "Transaction::new",
        "PersistHandle",
        "with_snapshot",
        "from_proto",
        "TableTransaction",
        "imbl::OrdMap",
        "OrdMap",
        "builtin_table_updates",
        "ddl",
        "controller_ready",
        "advance_timelines",
        "group_commit",
        "compute_controller",
        "ComputeController",
        "StorageController",
        "PersistClient",
        "tokio::runtime",
    ]

    print()
    print(f"{'substring':<45}{'samples':>10}{'% of total':>12}")
    print("-" * 67)
    for sub in substrings:
        ss = 0
        for addrs, c in stacks:
            for a in addrs:
                name = addr_to_name.get(a, a)
                if sub in name:
                    ss += c
                    break
        if ss > 0:
            print(f"{sub:<45}{ss:>10}{100*ss/total_samples:>11.2f}%")

    print()
    # Stack convention used here (jemalloc CPU profiler):
    #   addrs[0]   = deepest sampled frame (leaf)
    #   addrs[-1]  = perf_signal_handler / root
    # Many leaf frames are 0x0 (unresolved). For "self time", walk from
    # the leaf upward until we find an address that resolved to a name.
    def first_known(addrs):
        for a in addrs:
            n = addr_to_name.get(a)
            if n:
                return a, n
        return None, None

    self_by_name = defaultdict(int)
    for addrs, c in stacks:
        if not addrs:
            continue
        _, name = first_known(addrs)
        if name is None:
            self_by_name["<unresolved>"] += c
            continue
        # Strip the [...] hash suffix and trim
        name = re.sub(r"\[[0-9a-f]+\]", "", name)
        # Strip trailing "::<...>" generics for grouping
        name = re.sub(r"\\u003e", ">", name)
        name = re.sub(r"\\u003c", "<", name)
        self_by_name[name] += c

    print("Top 30 leaf symbols (self time):")
    print(f"{'symbol':<140}{'samples':>10}{'%':>8}")
    print("-" * 158)
    top = sorted(self_by_name.items(), key=lambda kv: -kv[1])[:30]
    for name, c in top:
        if len(name) > 138:
            name = name[:135] + "..."
        print(f"{name:<140}{c:>10}{100*c/total_samples:>7.2f}%")

    # Drill: for samples whose stack contains "create_materialized_view",
    # what are the leaf symbols?
    cmv_stacks = []
    for addrs, c in stacks:
        for a in addrs:
            name = addr_to_name.get(a, a)
            if "create_materialized_view" in name:
                cmv_stacks.append((addrs, c))
                break
    cmv_total = sum(c for _, c in cmv_stacks)
    print(f"\n=== {len(cmv_stacks)} stacks contain `create_materialized_view`, totaling {cmv_total} samples ===")

    cmv_leaf = defaultdict(int)
    for addrs, c in cmv_stacks:
        _, name = first_known(addrs)
        if name is None:
            cmv_leaf["<unresolved>"] += c
            continue
        name = re.sub(r"\[[0-9a-f]+\]", "", name)
        cmv_leaf[name] += c

    print("\nTop 25 leaf symbols within `create_materialized_view` stacks:")
    print(f"{'symbol':<140}{'samples':>10}{'%cmv':>8}")
    for name, c in sorted(cmv_leaf.items(), key=lambda kv: -kv[1])[:25]:
        if len(name) > 138:
            name = name[:135] + "..."
        print(f"{name:<140}{c:>10}{100*c/cmv_total:>7.2f}%")

    # Also: collapse top symbols by interesting Rust function names that
    # appear somewhere in the stack, but excluding pure tokio/poll noise
    interesting = [
        "sequence_create_materialized_view",
        "plan_create_materialized_view",
        "purify_create_materialized_view",
        "Catalog::transact",
        "Coordinator::catalog_transact",
        "CatalogState::apply_updates",
        "validate_resource_limits",
        "builtin_table_updates",
        "Transaction::commit",
        "Transaction::new",
        "TableTransaction::commit",
        "apply_to_snapshot_cache",
        "with_snapshot",
        "PersistHandle::commit",
        "from_proto",
        "advance_timelines",
        "compute_controller",
        "ComputeController::dataflow",
        "ComputeController::install_compute_collections",
        "StorageController::create_collections",
        "::resolve",
        "::optimize",
        "::transform",
        "::lower",
        "::plan_root_query",
        "imbl::ord::map::Node",
        "process_message",
        "send_immediate_rows",
        "purify",
        "session::Session",
        "sequence_inner",
        "stage_execute",
    ]
    print("\nInclusive samples WITHIN create_materialized_view stacks for selected sub-symbols:")
    print(f"{'substring':<55}{'samples':>10}{'%cmv':>8}")
    for sub in interesting:
        s = 0
        for addrs, c in cmv_stacks:
            for a in addrs:
                name = addr_to_name.get(a, a)
                if sub in name:
                    s += c
                    break
        if s > 0:
            print(f"{sub:<55}{s:>10}{100*s/cmv_total:>7.2f}%")


if __name__ == "__main__":
    main()
