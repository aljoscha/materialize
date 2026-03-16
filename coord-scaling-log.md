# Coordinator Scaling Investigation Log

## Session 1 — Setup & Code Analysis (2026-03-16)

- Created PROMPT.md, design doc (`misc/coord-linear-scaling.md`), reproducer
  script (`misc/coord-scaling-repro.sql`), and this log.
- Analyzed codebase to identify O(N) bottlenecks on the coordinator thread.
- **Key finding — B1 (table advancement in group_commit):**
  `src/adapter/src/coord/appends.rs:515-517` iterates ALL tables on every group
  commit to add "advancement" entries (empty writes that advance the table's
  upper). With 20k tables, this creates 20k entries in the appends map, which
  are then consolidated (20k iter), mapped to GlobalIds (20k catalog lookups),
  and sent to the persist worker (20k iter). All of this runs on the coordinator
  thread. Group commits fire on every INSERT, every DDL, and every ~1s via timer,
  so this explains why SELECT 1 is also affected (it gets queued behind a
  concurrent group commit).
- **Secondary finding — B2 (ReadHolds::downgrade):**
  After every group commit, `advance_timelines` calls `read_holds.downgrade()`
  which iterates all holds (one per table/source/MV/index per timeline). 20k
  iterations with channel sends.
- Next session: reproduce with the reproducer script, profile with samply to
  confirm B1 is the dominant cost, then fix.
