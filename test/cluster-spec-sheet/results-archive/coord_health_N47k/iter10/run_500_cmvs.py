"""Drive 500 CREATE MATERIALIZED VIEW statements on quickstart.

Each MV is unique (uses a different `id < N` filter) so the optimizer
can't short-circuit. The 500 names sit just past the last seeded
pad_mv index so they don't collide with existing MVs.
"""
import time
import psycopg

START = 100000  # well past the 47338 max pad_mv index
N = 500
DSN = "postgres://materialize@localhost:6875/materialize"

def main():
    with psycopg.connect(DSN, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute("SET cluster = quickstart")
        cur.execute("SET search_path = pad_schema")
        cur.execute("SET statement_timeout = '5min'")
        t0 = time.time()
        for i in range(N):
            idx = START + i
            cur.execute(
                f"CREATE MATERIALIZED VIEW iter10_load_{idx} AS "
                f"SELECT id, val FROM pad_base WHERE id < {idx}"
            )
            if (i + 1) % 50 == 0:
                dt = time.time() - t0
                print(f"{i+1}/{N} created  rate={(i+1)/dt:.2f}/s")
        dt = time.time() - t0
        print(f"DONE: {N} CMVs in {dt:.1f}s  rate={N/dt:.2f}/s")

if __name__ == "__main__":
    main()
