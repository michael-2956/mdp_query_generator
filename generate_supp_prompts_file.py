subgraph="""subgraph def_GROUP_BY {
        GROUP_BY [label="GROUP BY", shape=rectangle, style=filled, color=gray]
        EXIT_GROUP_BY [label="EXIT GROUP BY"]

        group_by_single_group [label="single group\\n(GROUP BY ())"]
        GROUP_BY -> group_by_single_group
        group_by_single_group -> EXIT_GROUP_BY

        has_accessible_columns [label="Has selectable columns\\n[set value]", set_value="has_accessible_cols"]
        GROUP_BY -> has_accessible_columns
        grouping_column_list [label="columns list\\n[call mod]", call_modifier="has_accessible_cols_mod"]
        has_accessible_columns -> grouping_column_list

        call1_column_spec[TYPES="[any]", label="TYPES: [any]\\nMODS: [no nesting, no literals, no formulas, no typed nulls,\\nno subquery, no aggregate, no case]", MODS="[no nesting, no literals, no formulas, no typed nulls, no subquery, no aggregate, no case]", shape=rectangle, style=filled, color=lightblue]
        grouping_column_list -> call1_column_spec
        call1_column_spec -> grouping_column_list
        call1_column_spec -> EXIT_GROUP_BY

        special_grouping [label="special grouping"]
        grouping_column_list -> special_grouping
        set_list [label="set list"]
        set_list_empty_allowed [label="Empty set\\n[call mod]", call_modifier="empty set allowed"]
        set_list -> set_list_empty_allowed
        set_list_empty_allowed -> set_list
        set_list_empty_allowed -> grouping_column_list
        set_list_empty_allowed -> EXIT_GROUP_BY

        grouping_rollup [label="rollup\\n[set value]", set_value="is_grouping_sets"]
        special_grouping -> grouping_rollup
        grouping_rollup -> set_list

        grouping_cube [label="cube\\n[set value]", set_value="is_grouping_sets"]
        special_grouping -> grouping_cube
        grouping_cube -> set_list

        grouping_set [label="grouping set\\n[set value]\\n(allows empty set)", set_value="is_grouping_sets"]
        special_grouping -> grouping_set
        grouping_set -> set_list

        call2_column_spec [TYPES="[any]", label="TYPES: [any]\\n MODS: [no nesting, no literals, no formulas, no typed nulls,\\nno subquery, no aggregate, no case]", MODS="[no nesting, no literals, no formulas, no typed nulls, no subquery, no aggregate, no case]", shape=rectangle, style=filled, color=lightblue]
        set_list -> call2_column_spec
        call2_column_spec -> set_list
        set_multiple [label="multiple cols in set"]
        call2_column_spec -> set_multiple
        set_multiple -> call2_column_spec
        set_multiple -> EXIT_GROUP_BY
        set_multiple -> grouping_column_list
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
