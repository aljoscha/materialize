#!/bin/bash

while true; do
    COMMIT=$(git rev-parse --short=6 HEAD)
    LOGFILE="agent_logs/agent_${COMMIT}.log"

    mkdir -p agent_logs

    echo "Logfile is $LOGFILE"

    claude --dangerously-skip-permissions \
           -p "$(cat pgwire-server-perf-prompt.md)" \
           --output-format stream-json --verbose \
           2>&1 | tee -a "$LOGFILE"
done
