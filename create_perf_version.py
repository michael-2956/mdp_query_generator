import sys

to_comment = """
FROM_cartesian_product -> call0_FROM_item

limit_num [label="limit [num]\\nmod: 'single row' -> OFF", modifier="single row", modifier_mode="off"]
LIMIT -> limit_num
call52_types [TYPES="[integer, numeric, bigint]", MODS="[no column spec, no aggregate]", label="TYPES: [integer, numeric, bigint]\\nMODS: [no column spec, no aggregate]", shape=rectangle, style=filled, color=lightblue]
limit_num -> call52_types
call52_types -> EXIT_LIMIT

types_value_typed_null [label="Typed null\\nmod.: 'no typed nulls' -> OFF", modifier="no typed nulls", modifier_mode="off"]
types_value -> types_value_typed_null
types_value_typed_null -> EXIT_types_value

set_list_empty_allowed -> set_list

call1_set_item -> set_list
"""

# SELECT_item_grouping_enabled -> SELECT_tables_eligible_for_wildcard

graph = open("graph.dot", 'r').read()

for line in to_comment.split('\n'):
    if line != '':
        new_line = "// " + line
        if graph.find(line) == -1:
            print(f"Failed to find:\n{line}", file=sys.stderr)
            exit(1)
        graph = graph.replace(line, new_line)

open("performance_untrained_graph.dot", 'w').write(graph)

