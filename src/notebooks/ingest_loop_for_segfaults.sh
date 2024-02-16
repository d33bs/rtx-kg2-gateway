#!/bin/bash
# Script to brute-force succeed with data ingest despite sporadic segmentation
# faults with Kuzu data ingest especially exhibited during certain complex 
# (many distinct node pair types) REL tables. Intended to be used manually.

# Command to run
COMMAND="poetry run python rtx_kg2_metanames_parquet_to_kuzu_copy_data_to_tables.py"

# Loop until a segmentation fault is detected
while true; do
    # Run the command
    $COMMAND
    
    # Check if the command exited with a segmentation fault
    if [ $? -ne 0 ]; then
        echo "Segmentation fault detected. Restarting the command."
    else
        # If it's not a segmentation fault, exit the loop
        break
    fi
done
