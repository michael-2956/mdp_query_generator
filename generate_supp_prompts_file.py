subgraph="""subgraph def_aggregate_function {
        aggregate_function [TYPES="[numeric, integer, bigint, 3VL Value, text, date, interval, timestamp]", label="aggregate function\\nTYPES=[numeric, integer, bigint, 3VL Value, text, date, interval, timestamp]", shape=rectangle, style=filled, color=peru]
        EXIT_aggregate_function [label="EXIT aggregate function"]

        aggregate_select_return_type [label="select\\nreturn type"]
        aggregate_not_distinct[label="no DISTINCT\\n[set value]", set_value="distinct_aggr"]
        aggregate_function -> aggregate_not_distinct
        aggregate_not_distinct -> aggregate_select_return_type
        aggregate_distinct[label="DISTINCT\\n[set value]", set_value="distinct_aggr"]
        aggregate_function -> aggregate_distinct
        aggregate_distinct -> aggregate_select_return_type


        aggregate_select_type_text[TYPE_NAME="text", label="text?"]
        aggregate_select_return_type -> aggregate_select_type_text

        arg_single_text [label="[text]"]
        aggregate_select_type_text -> arg_single_text
        call63_types [TYPES="[compatible]", label="TYPES: compatible(text)\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        arg_single_text -> call63_types
        call63_types -> EXIT_aggregate_function

        arg_double_text [label="[text, text]"]
        call74_types [TYPES="[compatible]", label="TYPES: compatible(text)\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        aggregate_select_type_text -> arg_double_text
        arg_double_text -> call74_types
        call74_types -> call63_types


        aggregate_select_type_numeric[TYPE_NAME="numeric", label="numeric?"]
        aggregate_select_return_type -> aggregate_select_type_numeric

        arg_single_numeric [label="[numeric]"]
        aggregate_select_type_numeric -> arg_single_numeric
        call66_types [TYPES="[compatible]", label="TYPES: compatible(numeric)\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        arg_single_numeric -> call66_types
        call66_types -> EXIT_aggregate_function
        
        arg_double_numeric [label="[numeric, numeric]"]
        aggregate_select_type_numeric -> arg_double_numeric   
        call68_types[TYPES="[compatible]", label="TYPES: compatible(numeric)\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        arg_double_numeric -> call68_types
        call68_types -> call66_types


        aggregate_select_type_bigint[TYPE_NAME="bigint", label="bigint?"]
        aggregate_select_return_type -> aggregate_select_type_bigint

        arg_bigint [label="[bigint]"]
        aggregate_select_type_bigint -> arg_bigint
        call75_types [TYPES="[compatible]", label="TYPES: compatible(bigint)\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        arg_bigint -> call75_types
        call75_types -> EXIT_aggregate_function

        arg_bigint_any [label="[any]"]
        aggregate_select_type_bigint -> arg_bigint_any
        call65_types [TYPES="[any]", label="TYPES: any\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        arg_bigint_any -> call65_types
        call65_types -> EXIT_aggregate_function

        arg_star [label="COUNT(*)\\n[call mod.]", call_modifier="distinct_aggr_mod"]
        aggregate_select_type_bigint -> arg_star
        arg_star -> EXIT_aggregate_function


        aggregate_select_type_integer[TYPE_NAME="integer", label="integer?"]
        aggregate_select_return_type -> aggregate_select_type_integer

        arg_integer [label="[integer]"]
        aggregate_select_type_integer -> arg_integer
        call71_types [TYPES="[compatible]", label="TYPES: compatible(integer)\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        arg_integer -> call71_types
        call71_types -> EXIT_aggregate_function


        aggregate_select_type_bool[TYPE_NAME="3VL Value", label="3VL Value?"]
        aggregate_select_return_type -> aggregate_select_type_bool

        arg_single_3vl [label="[3vl]"]
        aggregate_select_type_bool -> arg_single_3vl
        call64_types [TYPES="[compatible]", label="TYPES: compatible(3VL Value)\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        arg_single_3vl -> call64_types
        call64_types -> EXIT_aggregate_function


        aggregate_select_type_date [TYPE_NAME="date", label="date?"]
        aggregate_select_return_type -> aggregate_select_type_date

        arg_date [label="[date]"]
        aggregate_select_type_date -> arg_date
        call72_types [TYPES="[compatible]", label="TYPES: compatible(date)\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        arg_date -> call72_types
        call72_types -> EXIT_aggregate_function


        aggregate_select_type_timestamp [TYPE_NAME="timestamp", label="timestamp?"]
        aggregate_select_return_type -> aggregate_select_type_timestamp

        arg_timestamp [label="[timestamp]"]
        aggregate_select_type_timestamp -> arg_timestamp
        call96_types [TYPES="[compatible]", label="TYPES: compatible(timestamp)\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        arg_timestamp -> call96_types
        call96_types -> EXIT_aggregate_function


        aggregate_select_type_interval [TYPE_NAME="interval", label="interval?"]
        aggregate_select_return_type -> aggregate_select_type_interval

        arg_interval [label="[interval]"]
        aggregate_select_type_interval -> arg_interval
        call90_types [TYPES="[compatible]", label="TYPES: compatible(interval)\\nMODS: [no aggregate, aggregate-able columns]", MODS="[no aggregate, aggregate-able columns]", shape=rectangle, style=filled, color=lightblue]
        arg_interval -> call90_types
        call90_types -> EXIT_aggregate_function
    }"""

import re

print("[transitions]\n\n")
for line in subgraph.split("\n"):
    if re.compile(r"        .*\[.*\]").match(line) is not None:
        if line[8:].startswith("EXIT_"):
            continue
        node_name = line[8:].split("[")[0].strip()
        num_outgoing = len(list(filter(lambda x: re.compile(f"        {node_name}\s*->").match(x) is not None, subgraph.split("\n"))))
        if num_outgoing > 1:
            print(f"[transitions.{node_name}]")
            print('task="""')
            print('"""')
            print(f"[transitions.{node_name}.options]")
            for i in range(num_outgoing):
                print(f'{i+1}=""')
            print(f"[transitions.{node_name}.option_nodes]")
            for i, tr_line in enumerate(filter(lambda x: re.compile(f"        {node_name}\s*->").match(x) is not None, subgraph.split("\n"))):
                to_node_name = tr_line.split("->")[1].strip()
                print(f'{i+1}="{to_node_name}"')
            print('\n')

print("[call_node_context]")
for line in subgraph.split("\n"):
    if re.compile(r"        .*\[.*\]").match(line) is not None:
        node_name = line[8:].split("[")[0].strip()
        if re.compile(r"call[0-9]+_.*").match(node_name) is not None:
            print(f'{node_name}=""')
print('\n')

print("[value_chooser_tasks]")
