# Observe a deterministic transaction

Run this against an **already running Materialize build with deterministic
transactions**. Use a writable database where your role can create schemas
and tables, and a cluster you can query. Use Python 3 with psycopg 3 in your
client environment. No repository dependencies or server changes are needed.

To start the server in a fresh worktree, set `MZDEV_POSTGRES` to a dedicated
PostgreSQL database and run `misc/demos/deterministic-transactions/start`.
The launcher binds SQL to loopback and opts the fresh catalog into txn-WAL.
Reuse the same worktree and database on restart. Never pass `--reset` to
restart the demo. See [the storage contract](../../../src/catalog/DEMO-WAL.md).

From this directory (or use the repository's `bin/pyactivate` in place of
`python3`):

```sh
export MZ_URL='postgresql://materialize@localhost:6875/materialize'
python3 demo.py run
```

The runner opens two sessions. It creates a UUID-named schema, follows its
catalog ID through two renames, and reads both table shards in one query to
check the exact row multiplicities. The observer checks state before and after commit, after an empty
commit, after rollback, and after it claims a rename destination from the writer.
A mismatch or unexpected SQL error exits nonzero. Nothing is dropped, even
on failure. Use a quiet database to avoid unrelated concurrent catalog changes.

Save the printed `python3 demo.py inspect ...` command. After a server process
restart **using the same durable state**, run that command from this directory
with the same URL. It only reads: it checks both schema IDs, absence of old and
intermediate names, and exactly one copy each of rows 1 and 2. Running `run`
again creates a different example and does not verify the original.

## Race the cluster candidate before its WAL compare-and-set

Use `competition.py` with the cluster candidate implementation on a dedicated
local instance. The failpoint is process-wide. Run only one deterministic
writer. Use the same txn-WAL setup described above, enable unsafe mode, and
keep INFO coordinator events in a private, uncolored text log. For example,
from this directory, with `MZDEV_POSTGRES` set:

```sh
umask 077
export SERVER_LOG=$(mktemp)
UNSAFE_MODE=1 LOG_FORMAT=text NO_COLOR=1 ./start >>"$SERVER_LOG" 2>&1
```

In another shell, set `SERVER_LOG` to the same path and `MZ_URL` to the demo
database, then run:

```sh
python3 competition.py run --server-log "$SERVER_LOG"
```

The runner seeks to EOF before each COMMIT. It waits for the first candidate
read, commits the competing write, then releases the pause. Do not rotate the
log during the run. The runner reports only the arrangement ID, attempt and
write timestamp from the log. Do not share the log itself.

The source insert forces a reread on the same arrangement. The destination
and both dependent views must return 1 and 2 exactly once. The outside
view's stored CREATE SQL must reference the renamed schema. In the second
transaction the competitor creates the claimed schema. Only an explicit
already-exists error for that schema passes. The claimant keeps its ID and
neither the rename nor the staged insert may survive.

Restart with the same durable state, then use the IDs printed by the runner:

```sh
python3 competition.py inspect PREFIX SCHEMA_ID OUTSIDE_ID CLAIMANT_ID
```

Inspection reads only the catalog, tables and views. If the client was killed
while the hook was armed, clear it on the dedicated server before rerunning:

```sql
SET unsafe_failpoints = 'deterministic_candidate_before_cas=off';
```

## Try it in two psql sessions

In a shell, generate fresh names and start the following command twice (use
the **same** shell variables in both terminals):

```sh
p=dt_$(python3 -c 'import uuid; print(uuid.uuid4().hex)')
psql -X "$MZ_URL" -v old="${p}_old" -v mid="${p}_mid" -v last="${p}_final" -v claim="${p}_claim"
```

Run the blocks in order. Leave the observer in psql's default autocommit mode.

```sql
-- Writer
CREATE SCHEMA :"old";
CREATE TABLE :"old".numbers (n int);
CREATE TABLE :"old".archive (n int);
BEGIN DETERMINISTIC TRANSACTION;
ALTER SCHEMA :"old" RENAME TO :"mid";
INSERT INTO :"mid".numbers VALUES (1);
ALTER SCHEMA :"mid" RENAME TO :"last";
INSERT INTO :"last".numbers SELECT n + 1 FROM :"last".numbers;
INSERT INTO :"last".archive SELECT n FROM :"last".numbers;
```

```sql
-- Observer: note the ID. Only the old schema exists, and its table is empty.
SELECT s.id, s.name FROM mz_catalog.mz_schemas s
JOIN mz_catalog.mz_databases d ON d.id = s.database_id
WHERE d.name = current_database()
  AND s.name IN (:'old', :'mid', :'last', :'claim');
SELECT n FROM :"old".numbers;
```

```sql
-- Writer
COMMIT;
```

In the observer, repeat the catalog query: only the final name should appear,
with the original ID. Run `SELECT n FROM :"last".numbers ORDER BY n;`: expect
1 and 2. The second insert read the first insert through the renamed schema.

```sql
-- Writer
BEGIN DETERMINISTIC TRANSACTION;
UPDATE :"last".numbers SET n = n + 10;
ALTER SCHEMA :"last" RENAME TO :"mid";
ROLLBACK;
```

Repeat both observer checks. Neither rows nor names should change. Now force
a conflict without a timing race:

```sql
-- Writer (do not commit yet)
BEGIN DETERMINISTIC TRANSACTION;
INSERT INTO :"last".numbers VALUES (99);
ALTER SCHEMA :"last" RENAME TO :"claim";
```

```sql
-- Observer
CREATE SCHEMA :"claim";
```

```sql
-- Writer: must fail because the destination is no longer available.
COMMIT;
```

Repeat the observer checks: the final schema retains its ID, the claimed name
has its own ID, and the table still contains only 1 and 2. Do not retry the
failed program against the changed namespace.

## Constraints

Inside the block, use schema renames and deterministic INSERT, UPDATE, or
DELETE on writable user tables. A later mutation can read earlier mutations.
Standalone SELECT, arbitrary DDL, RETURNING, unstable expressions and
order-sensitive operations such as LIMIT are not supported. Set up objects
outside the block and inspect them through the observer. A successful staged
statement does not mean the block will commit.

The dataflow maintains a *candidate*, not a continual write. Only the
coordinator's successful compare-and-append publishes it. Retries read the
same arrangement. They do not install a new dataflow.

The comparison covers the whole WAL, not just the transaction's inputs.
Unrelated writes or other commit attempts can exhaust the 32-attempt limit.
An unrelated catalog change can also invalidate a bound mutation. Use a
dedicated cluster and database for this demo. If compute stops advancing,
the internal peek does not yet implement client cancellation or a timeout.
