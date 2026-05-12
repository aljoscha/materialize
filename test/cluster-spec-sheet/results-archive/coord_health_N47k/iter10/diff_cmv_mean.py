"""Compute the mean per-call duration for create_materialized_view_stage_ready
between pre and post /metrics snapshots, using sum/count diff."""
import re
import sys

PRE = "/tmp/iter10/snap_pre.prom"
POST = "/tmp/iter10/snap_post.prom"

def grab(path, family, label_eq):
    sum_v, cnt_v = None, None
    pat_sum = re.compile(rf'^{family}_sum\{{(.*)\}}\s+([0-9.eE+-]+)')
    pat_cnt = re.compile(rf'^{family}_count\{{(.*)\}}\s+([0-9.eE+-]+)')
    with open(path) as f:
        for line in f:
            m = pat_sum.match(line)
            if m and label_eq in m.group(1):
                sum_v = float(m.group(2))
            m = pat_cnt.match(line)
            if m and label_eq in m.group(1):
                cnt_v = float(m.group(2))
    return sum_v, cnt_v

family = "mz_slow_message_handling"
label_eq = 'message_kind="create_materialized_view_stage_ready"'
pre_sum, pre_cnt = grab(PRE, family, label_eq)
post_sum, post_cnt = grab(POST, family, label_eq)
print(f"pre  count={pre_cnt} sum={pre_sum}")
print(f"post count={post_cnt} sum={post_sum}")
ds = post_sum - pre_sum
dc = post_cnt - pre_cnt
print(f"diff count={dc} sum={ds:.3f}s")
if dc > 0:
    print(f"mean per call = {1000*ds/dc:.2f} ms  (over {int(dc)} calls)")

# Also the append duration mean
family = "mz_append_table_duration_seconds"
label_eq = ""  # only has bucket labels
def grab_simple(path, family):
    with open(path) as f:
        sm = cn = None
        for line in f:
            if line.startswith(family + "_sum "):
                sm = float(line.split()[-1])
            elif line.startswith(family + "_count "):
                cn = float(line.split()[-1])
    return sm, cn

ps, pc = grab_simple(PRE, family)
qs, qc = grab_simple(POST, family)
ds, dc = qs - ps, qc - pc
print(f"\nappend_table: count={int(dc)}, sum={ds:.3f}s, mean={1000*ds/dc:.2f} ms" if dc else "no appends")
