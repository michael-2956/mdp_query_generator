#!/bin/zsh

set -e

if test -f parser_bugs.sql; then
  rm parser_bugs.sql
fi

python3 create_perf_version.py 

cargo run --release -- configs/only_queries_config.toml -n 10000 > generated_queries.sql

psql tpch -f generated_queries.sql 2>&1 >/dev/null

if test -f parser_bugs.sql; then
  # echo "=== Next testing assumed parser bugs for ERRORs ==="
  psql tpch -f parser_bugs.sql 2>&1 >/dev/null
fi