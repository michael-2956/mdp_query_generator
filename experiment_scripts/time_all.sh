#!/bin/zsh

n_queries=10000

# min_time=999999
# for i in {1..10}
# do
#     # experiments/speed_eval/seed_0_anticall_$i.sql
#     current_time=$( { time ./target/release/equivalence_testing configs/only_queries_config.toml -n $n_queries &> /dev/null; } 2>&1 | awk '{print $11}' )

#     if (( $(echo "$current_time < $min_time" | bc -l) )); then
#         min_time=$current_time
#     fi

#     echo $current_time
# done

# echo "Min time (ours): $min_time seconds"
# echo

# sleep 120  # cool down

# min_time=999999
# for i in {1..10}
# do
#     current_time=$( { time ../sqlsmith/sqlsmith --dry-run --target="host=localhost port=5432 dbname=tpch" --max-queries=$n_queries --seed=0 &> /dev/null; } 2>&1 | awk '{print $12}' )

#     if (( $(echo "$current_time < $min_time" | bc -l) )); then
#         min_time=$current_time
#     fi

#     echo $current_time
# done

# echo "Min time (SQLSmith): $min_time seconds"

# ./target/release/equivalence_testing configs/only_queries_config.toml -n $n_queries > experiment_results/our_10K_balanced.txt
# ../sqlsmith/sqlsmith --dry-run --target="host=localhost port=5432 dbname=tpch" --max-queries=$n_queries --seed=0 2>/dev/null > experiment_results/sqlsmith_10K.txt

# echo
# echo "Our node mean:"
# cat experiment_results/our_10K_balanced.txt | python3 experiment_scripts/count_ast_nodes.py

# echo
# echo "SQLSmith node mean:"
# cat experiment_results/sqlsmith_10K.txt | python3 experiment_scripts/count_ast_nodes.py

echo "Minimum Times for Each i:" > experiment_results/anticall_min_time_per_setting_10K.txt

for i in {1..12}
do
   min_time=999999
   for j in {1..3}
   do
      echo n: $n_queries --anticall_stir_level $i j: $j
      current_time=$( {\time -p ./target/release/equivalence_testing configs/only_queries_config.toml -n $n_queries --anticall_stir_level $i > /dev/null} 2>&1 | grep real | awk '{print $2}' )
      echo tm: $current_time

      if (( $(echo "$current_time < $min_time" | bc -l) )); then
         min_time=$current_time
      fi

   done

   echo "i=$i, Min Time: $min_time seconds" >> experiment_results/anticall_min_time_per_setting_10K.txt
done
