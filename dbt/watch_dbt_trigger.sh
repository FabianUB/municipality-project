#!/bin/bash

# Script to watch for dbt trigger file and execute dbt build when triggered
# This runs in the dbt container to respond to Dagster requests

DBT_PROJECT_DIR="/app/dbt"
TRIGGER_FILE="$DBT_PROJECT_DIR/run_dbt_build.trigger"
RESULT_FILE="$DBT_PROJECT_DIR/dbt_build_result.txt"

echo "Starting dbt trigger watcher..."
echo "Watching for trigger file: $TRIGGER_FILE"

while true; do
    if [ -f "$TRIGGER_FILE" ]; then
        echo "Trigger file detected! Running dbt build..."
        
        # Change to dbt project directory
        cd "$DBT_PROJECT_DIR"
        
        # Run dbt build and capture result
        if dbt build > /tmp/dbt_output.log 2>&1; then
            echo "SUCCESS: dbt build completed successfully" > "$RESULT_FILE"
            echo "--- dbt output ---" >> "$RESULT_FILE"
            cat /tmp/dbt_output.log >> "$RESULT_FILE"
        else
            echo "FAILED: dbt build failed" > "$RESULT_FILE"
            echo "--- dbt output ---" >> "$RESULT_FILE"
            cat /tmp/dbt_output.log >> "$RESULT_FILE"
        fi
        
        # Remove trigger file
        rm -f "$TRIGGER_FILE"
        
        echo "dbt build completed, result written to $RESULT_FILE"
    fi
    
    # Wait 5 seconds before checking again
    sleep 5
done