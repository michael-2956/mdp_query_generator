subgraph="""subgraph def_Query {
        Query [TYPES="[numeric, integer, bigint, 3VL Value, text, date, interval, timestamp]", MODS="[single column, single row]", label="Query\ntypes=[numeric, integer, bigint, 3VL Value, text, date, interval, timestamp]\nmods=[single column, single row]", shape=octagon, style=filled, color=green]
        EXIT_Query [label="EXIT Query", shape=rectangle]

        call0_FROM [label="FROM", shape=rectangle, color=cornflowerblue, style=filled]
        Query -> call0_FROM

        call0_WHERE [label="WHERE", shape=rectangle, style=filled, color=darkgoldenrod1]
        call0_FROM -> call0_WHERE

        call0_SELECT [label="SELECT\nTYPES: [...types]\nMODS: [?single column]", TYPES="[...]", MODS="[?single column]", shape=rectangle, style=filled, color=bisque]
        call0_WHERE -> call0_SELECT
        call0_FROM -> call0_SELECT
        
        call0_GROUP_BY [label="GROUP BY", shape=rectangle, style=filled, color=gray]
        call0_WHERE -> call0_GROUP_BY
        call0_FROM -> call0_GROUP_BY
        call0_GROUP_BY -> call0_SELECT

        call0_HAVING [label="HAVING", shape=rectangle, style=filled, color=mediumvioletred]
        call0_GROUP_BY -> call0_HAVING
        call0_HAVING -> call0_SELECT

        call0_ORDER_BY [label="ORDER BY", shape=rectangle, style=filled, color=deepskyblue]
        call0_SELECT -> call0_ORDER_BY

        call0_LIMIT [label="LIMIT\nMODS: [?single row]", MODS="[?single row]", shape=rectangle, style=filled, color=brown]
        call0_ORDER_BY -> call0_LIMIT
        call0_LIMIT -> EXIT_Query
    }"""

import re

for line in subgraph.split("\n"):
    if re.compile(r"        .*\[.*\]").match(line) is not None:
        if line[8:].startswith("EXIT_"):
            continue
        node_name = line[8:].split("[")[0].strip()
        num_outgoing = len(list(filter(lambda x: re.compile(f"        {node_name}.*->").match(x) is not None, subgraph.split("\n"))))
        if num_outgoing > 1:
            print(f"[transitions.{node_name}]")
            print('task="""')
            print('"""')
            print(f"[transitions.{node_name}.options]")
            for i in range(num_outgoing):
                print(f'{i+1}=""')
            print(f"[transitions.{node_name}.option_nodes]")
            for i, tr_line in enumerate(filter(lambda x: re.compile(f"        {node_name}.*->").match(x) is not None, subgraph.split("\n"))):
                to_node_name = tr_line.split("->")[1].strip()
                print(f'{i+1}="{to_node_name}"')
            print('\n')

#         print(f'''
# [transitions.{node_name}]
# task="""
# """
# [transitions.{node_name}.options]
# 1=""
# 2=""
# [transitions.{node_name}.option_nodes]
# 1=""
# 2=""
# ''')