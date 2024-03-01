#!/bin/zsh

set -e

if test -f parser_bugs.sql; then
  rm parser_bugs.sql
fi

python3 create_perf_version.py

# train the model
# cargo run --release -- training_config.toml
# model is saved to weights/subgraph/unstacked/tpch_default_queries.mw

cargo run -- run_model_config.toml -n 100 > generated_queries.sql

# test the generated queries
psql tpch -f generated_queries.sql 2>&1 >/dev/null

if test -f parser_bugs.sql; then
  # echo "=== Next testing assumed parser bugs for ERRORs ==="
  psql tpch -f parser_bugs.sql 2>&1 >/dev/null
fi