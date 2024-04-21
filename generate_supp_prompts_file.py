subgraph="""subgraph def_SELECT {
        SELECT [TYPES="[numeric, integer, bigint, 3VL Value, text, date, interval, timestamp]", MODS="[single column]", label="SELECT\\ntypes=[numeric, integer, bigint, 3VL Value, text, date, interval, timestamp]\\nmods=[single column]", shape=rectangle, style=filled, color=bisque]
        EXIT_SELECT [label="EXIT SELECT", shape=rectangle]

        SELECT_DISTINCT [label="DISTINCT"]
        SELECT -> SELECT_DISTINCT

        SELECT_list [label="SELECT list\\nset value: 'grouping_enabled'", set_value="grouping_enabled"]
        SELECT_DISTINCT -> SELECT_list
        SELECT -> SELECT_list
        SELECT_list_multiple_values [label="Multiple values\\nmod.: 'single column' -> OFF", modifier="single column", modifier_mode="off"]
        SELECT_list_multiple_values -> SELECT_list

        SELECT_unnamed_expr [label="Unnamed"]
        SELECT_expr_with_alias [label="With alias"]
        SELECT_list -> SELECT_unnamed_expr
        SELECT_list -> SELECT_expr_with_alias

        select_expr [label="Expression"]
        SELECT_unnamed_expr -> select_expr
        SELECT_expr_with_alias -> select_expr
        select_expr_done [label="Expression done"]
        select_expr_done -> SELECT_list_multiple_values
        select_expr_done -> EXIT_SELECT
        call73_types [label="TYPES: [...types]\\nMODS: [group by columns]\\ncall mod.: 'grouping mode switch'", TYPES="[...]", MODS="[group by columns]", call_modifier="grouping mode switch", shape=rectangle, style=filled, color=lightblue]
        select_expr -> call73_types
        call73_types -> select_expr_done
        call54_types [label="TYPES: [...types]\\nMODS: [no aggregate]\\ncall mod.: 'grouping mode switch'", TYPES="[...]", MODS="[no aggregate]", call_modifier="grouping mode switch", shape=rectangle, style=filled, color=lightblue]
        select_expr -> call54_types
        call54_types -> select_expr_done

        SELECT_tables_eligible_for_wildcard [label="Set Relations for wildcards\\nset_value='wildcard_relations'", set_value="wildcard_relations"]
        SELECT_list -> SELECT_tables_eligible_for_wildcard

        SELECT_wildcard [label="wildcard\\ncall mod.: is_wildcard_available", call_modifier="is_wildcard_available"]
        SELECT_tables_eligible_for_wildcard -> SELECT_wildcard
        SELECT_wildcard -> SELECT_list_multiple_values
        SELECT_wildcard -> EXIT_SELECT

        SELECT_qualified_wildcard [label="qualified wildcard\\ncall mod.: is_wildcard_available", call_modifier="is_wildcard_available"]
        SELECT_tables_eligible_for_wildcard -> SELECT_qualified_wildcard
        SELECT_qualified_wildcard -> SELECT_list_multiple_values
        SELECT_qualified_wildcard -> EXIT_SELECT
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
