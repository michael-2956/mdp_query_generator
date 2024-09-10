#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 path/to/database.sqlite"
    exit 1
fi

db_path="$1"
db_dir=$(dirname "$db_path")
schema_path="$db_dir/schema.sql"

sqlite3 "$db_path" .schema > "$schema_path"

echo "Schema saved to $schema_path"

