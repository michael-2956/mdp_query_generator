#!/bin/bash

for i in {1..12}
do
   ./target/release/equivalence_testing configs/only_queries_config.toml -n 10000 --anticall_stir_level $i > experiments/speed_eval/seed_0_anticall_$i.sql &
done

wait
echo "Generated different stirs."

for i in {1..12}
do
   echo $i
   cat experiments/speed/seed_0_anticall_$i.sql | python3 count_ast_nodes.py
done
