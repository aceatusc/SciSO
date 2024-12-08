#!/bin/bash
# Imports all success.jsonl files in all catalogs/ subdirectories into the pub_urls collection

# Path to the root directory containing subdirectories
ROOT_DIR="<absolute_path_to_catalogs>"

# MongoDB database and collection
DB_NAME="<database_name>"
COLLECTION_NAME="pub_urls"

ROOT_DIR=$(eval echo $ROOT_DIR)
# Iterate through all subdirectories
for SUBDIR in "$ROOT_DIR"/*; do
  if [ -d "$SUBDIR" ]; then # Check if it's a directory
    echo "Processing directory: $SUBDIR"

    # Check if success.jsonl exists in the current subdirectory
    SUCCESS_FILE="$SUBDIR/success.jsonl"
    if [[ -f "$SUCCESS_FILE" ]]; then
      echo "Found $SUCCESS_FILE. Importing into MongoDB..."
      # Import the success.jsonl file into MongoDB
      mongoimport --db "$DB_NAME" --collection "$COLLECTION_NAME" --file "$SUCCESS_FILE"
    else
      echo "No success.jsonl found in $SUBDIR. Skipping."
    fi
  fi
done