# Observe a deterministic transaction

Run this against an **already running Materialize build with deterministic
transactions**. Use a writable database where your role can create schemas
and tables, and a cluster you can query. Use Python 3 with psycopg 3 in your
client environment. No repository dependencies or server changes are needed.

From this directory:

```sh
export MZ_URL='postgresql://materialize@localhost:6875/materialize'
python3 demo.py run
```

The runner opens two sessions. It creates a UUID-named schema, follows its
catalog ID through two renames, and checks the full ordered row list, including
duplicates. The observer checks state before and after commit, after an empty
commit, after rollback, and after it claims a rename destination from the writer.
A mismatch or unexpected SQL error exits nonzero. Nothing is dropped, even
on failure. Use a quiet database to avoid unrelated concurrent catalog changes.

Save the printed `python3 demo.py inspect ...` command. After a server process
restart **using the same durable state**, run that command from this directory
with the same URL. It only reads: it checks both schema IDs, absence of old and
intermediate names, and exactly one copy each of rows 1 and 2. Running `run`
again creates a different example and does not verify the original.

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
BEGIN DETERMINISTIC TRANSACTION;
ALTER SCHEMA :"old" RENAME TO :"mid";
INSERT INTO :"mid".numbers VALUES (1);
ALTER SCHEMA :"mid" RENAME TO :"last";
INSERT INTO :"last".numbers SELECT n + 1 FROM :"last".numbers;
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
