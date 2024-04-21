subgraph="""subgraph def_FROM_item {
        FROM_item [label="FROM item", shape=rectangle, style=filled, color=lightgreen]
        EXIT_FROM_item [label="EXIT FROM item"]

        FROM_item_alias [label="with alias\\n[set value]", set_value="available_table_names"]
        FROM_item -> FROM_item_alias

        FROM_item_no_alias [label="without alias\\n[set value]", set_value="available_table_names"]
        FROM_item -> FROM_item_no_alias

        FROM_item_table [label="Table\\n[call mod]", call_modifier="from_table_names_available"]
        FROM_item_no_alias -> FROM_item_table
        FROM_item_alias -> FROM_item_table
        FROM_item_table -> EXIT_FROM_item

        call0_Query [label="Query", shape=rectangle, TYPES="[any]", MODS="[]", style=filled, color=green]
        FROM_item_alias -> call0_Query
        call0_Query -> EXIT_FROM_item
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
