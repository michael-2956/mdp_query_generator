#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 /path/to/spider/"
    exit 1
fi

base_dir="$1"
schema_dir="$base_dir/schemas"

mkdir -p "$schema_dir"

find "$base_dir" -type f -name "*.sqlite" | while read -r db_path; do
    db_name=$(basename "$db_path" .sqlite)

    schema_path="$schema_dir/$db_name.sql"

    sqlite3 "$db_path" .schema > "$schema_path"

    echo "Schema for $db_name saved to $schema_path"
done

