#!/bin/bash

echo "Minimum Times for Each i:" > min_times_results.txt

for i in {1..12}
do
   min_time=999999
   for j in {1..10}
   do
      # experiments/speed_eval/seed_0_anticall_$i.sql
      current_time=$( { time ./target/release/equivalence_testing configs/only_queries_config.toml -n 100 --anticall_stir_level $i > /dev/null; } 2>&1 | awk '{print $11}' )

      if (( $(echo "$current_time < $min_time" | bc -l) )); then
         min_time=$current_time
      fi
   done

   echo "i=$i, Min Time: $min_time seconds" >> min_times_results.txt
done
