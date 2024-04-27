import sqlparse

raw = ""
while True:
    try:
        raw += f"{input()}\n"
    except:
        break

statements = raw.split(';')

num_characters = 0
num_tokens = 0

def count_nodes(parsed_stmt):
    return sum([
        1 if not token.is_group else (count_nodes(token) + 1)
        for token in parsed_stmt.tokens
        if not token.is_whitespace
    ])

# filter out empty ones
statements = list(filter(lambda s: len(s.strip()) != 0, statements))

for i, statement in enumerate(statements):
    if i % 10 == 0:
        print(f"{i}/{len(statements)}", flush=True, end='    \r')
    parsed = sqlparse.parse(statement)
    if len(parsed) != 1:
        print("Failed to parse:", statement)
        print("Number of statements:", len(parsed))
    num_tokens += count_nodes(parsed[0])
    num_characters += len(statement)

print(f"Mean number of nodes: {num_tokens/len(statements)}")
print(f"Mean number of charaters: {num_characters/len(statements)}")
print(f"Ratio (chars per token): {num_characters/num_tokens}")
