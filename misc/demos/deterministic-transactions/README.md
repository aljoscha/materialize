# Record a deterministic transaction in two psql sessions

Both demos pause the writer after compute has returned its first result but
before the coordinator can commit it. Change an input in the other session,
then watch the writer retry on the **same subscription ID**.

## Start the demo

From the repository root, point `MZDEV_POSTGRES` at your usual **throwaway**
PostgreSQL database. Stop other local Materialize instances first: `--reset`
drops the Materialize metadata schemas and can kill your other local
`clusterd` processes. Run this on an isolated dev machine:

```sh
export MZ_DEMO_CATALOG_TXN_WAL=1
umask 077
LOG_FORMAT=text NO_COLOR=1 bin/environmentd --reset -- --unsafe-mode 2>&1 | tee server.log
```

Leave it running. Omit `--reset` on restart to keep your data. Open two
terminals, **A (writer)** and **B (competitor)**, and run this in each:

```sh
psql -X postgresql://materialize@127.0.0.1:6875/materialize
```

For the video, keep the raw server log off screen. In a small third pane,
show only the candidate reads:

```sh
tail -n 0 -f server.log | grep --line-buffered -oE 'deterministic candidate read subscribe=t[0-9]+ attempt=[0-9]+ write_ts=[0-9]+'
```

## Take 1: a new row changes the answer

**A** creates a source with one row and an empty destination. The transaction
renames the schema, then copies the source using its new name.

```sql
CREATE SCHEMA take1_old;
CREATE TABLE take1_old.src (n int);
CREATE TABLE take1_old.dst (n int);
INSERT INTO take1_old.src VALUES (1);

SET emit_plan_insights_notice = on;
BEGIN DETERMINISTIC TRANSACTION;
ALTER SCHEMA take1_old RENAME TO take1_final;
INSERT INTO take1_final.dst SELECT n FROM take1_final.src;
```

**B** arms the pause:

```sql
SET unsafe_failpoints = 'deterministic_candidate_before_cas=return';
```

**A** commits:

```sql
COMMIT;
```

A prints the physical plan as a notice, including literal values. **Wait
for `attempt=1` in the log pane**, not just the notice. Now **B** can still
see the old name and the empty destination. Insert a row and release A:

```sql
SELECT * FROM take1_old.dst;  -- zero rows
INSERT INTO take1_old.src VALUES (2);
SET unsafe_failpoints = 'deterministic_candidate_before_cas=off';
```

**A** sees `COMMIT` succeed. The log shows `attempt=2` with the same
subscription ID. Row 2 arrived after the first read, but both rows were
copied exactly once:

```sql
SELECT n FROM take1_final.dst ORDER BY n;  -- 1, 2
```

## Take 2: a competing schema name rejects the whole transaction

Continue in the same sessions. **B** arms the pause again:

```sql
SET unsafe_failpoints = 'deterministic_candidate_before_cas=return';
```

**A** tries to rename the schema and insert a row under its new name:

```sql
BEGIN DETERMINISTIC TRANSACTION;
ALTER SCHEMA take1_final RENAME TO take1_claim;
INSERT INTO take1_claim.dst VALUES (99);
COMMIT;
```

Wait for this transaction's `attempt=1`. **B** takes the name and releases A:

```sql
CREATE SCHEMA take1_claim;
SET unsafe_failpoints = 'deterministic_candidate_before_cas=off';
```

**A** gets `schema 'take1_claim' already exists`. The second read used the
same subscription as the first read of this transaction. The original name
still works, and row 99 was never published:

```sql
SELECT n FROM take1_final.dst ORDER BY n;  -- still 1, 2
```

If you interrupt either take, run the `=off` statement in B. The pause is
process-wide. To repeat without resetting, choose fresh schema names.
