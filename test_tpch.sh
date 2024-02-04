#!/bin/zsh

rm parser_bugs.sql

cargo run --release -- only_queries_config.toml -n 10000 > generated_queries.sql

psql tpch -f generated_queries.sql 2>&1 >/dev/null
# echo "=== Next testing assumed parser bugs for ERRORs ==="
psql tpch -f parser_bugs.sql 2>&1 >/dev/null