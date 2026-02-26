#!/bin/bash
# Benchmark: CREATE TABLE FROM SOURCE in a single transaction
# Tests increasing batch sizes to see scaling behavior and where it breaks.
# Uses non-overlapping upstream table references — no drops needed between runs.
set -euo pipefail

MZ_HOST="localhost"
MZ_PORT="6875"
MZ_USER="materialize"
MZ_DB="materialize"
PSQL="psql -U $MZ_USER -h $MZ_HOST -p $MZ_PORT $MZ_DB -v ON_ERROR_STOP=1"

# Batch sizes to test
BATCH_SIZES=(1 5 10 25 50 100 200 300 500)

# Number of repetitions per batch size
REPS=3

# Counter for upstream table allocation (non-overlapping)
NEXT_TABLE=1

echo "=== CREATE TABLE FROM SOURCE Transaction Benchmark ==="
echo "Optimized build, local filesystem persist, Docker CockroachDB"
echo "Non-overlapping upstream refs, no drops between runs"
echo "Repetitions per batch size: $REPS"
echo ""

# Verify source
REF_COUNT=$($PSQL -t -c "SELECT count(*) FROM mz_internal.mz_source_references WHERE source_id = (SELECT id FROM mz_sources WHERE name = 'pg_source');" 2>/dev/null | tr -d ' ')
echo "Available source references: $REF_COUNT"

# Compute total tables needed
TOTAL_NEEDED=0
for BATCH in "${BATCH_SIZES[@]}"; do
    TOTAL_NEEDED=$((TOTAL_NEEDED + BATCH * REPS))
done
echo "Total tables needed: $TOTAL_NEEDED"
if [ $TOTAL_NEEDED -gt $REF_COUNT ]; then
    echo "WARNING: not enough upstream tables for all batches!"
fi
echo ""

echo "batch_size | rep | total_ms | per_table_ms | status"
echo "-----------|-----|----------|--------------|-------"

for BATCH in "${BATCH_SIZES[@]}"; do
    for REP in $(seq 1 $REPS); do
        END_TABLE=$((NEXT_TABLE + BATCH - 1))
        if [ $END_TABLE -gt $REF_COUNT ]; then
            echo "$BATCH | $REP | - | - | SKIPPED (exhausted upstream tables at $NEXT_TABLE)"
            continue
        fi

        # Build the SQL transaction
        SQL="BEGIN;\n"
        for i in $(seq $NEXT_TABLE $END_TABLE); do
            SQL+="CREATE TABLE mz_bench_$i FROM SOURCE pg_source (REFERENCE \"public\".\"bench_table_$i\");\n"
        done
        SQL+="COMMIT;\n"

        # Run and time it
        START_NS=$(date +%s%N)
        RESULT=$(echo -e "$SQL" | $PSQL 2>&1) || {
            ELAPSED_MS=$(( ($(date +%s%N) - START_NS) / 1000000 ))
            echo "$BATCH | $REP | ${ELAPSED_MS} | - | FAILED"
            echo "  Error: $(echo "$RESULT" | grep -i 'error' | head -3)"
            # Still advance to avoid re-using refs
            NEXT_TABLE=$((END_TABLE + 1))
            continue
        }
        END_NS=$(date +%s%N)
        ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
        PER_TABLE=$((ELAPSED_MS / BATCH))

        echo "$BATCH | $REP | ${ELAPSED_MS} | ${PER_TABLE} | OK"

        NEXT_TABLE=$((END_TABLE + 1))
    done
done

echo ""
echo "=== Summary ==="
echo "Total tables created: $((NEXT_TABLE - 1))"
$PSQL -t -c "SELECT count(*) AS mz_bench_tables FROM mz_tables WHERE name LIKE 'mz_bench_%';" 2>/dev/null | tr -d ' '
echo "=== Done ==="
