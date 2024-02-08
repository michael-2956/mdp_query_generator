from termcolor import colored

ignore_errs = [
    "a negative number raised to a non-integer power yields a complex result",
    "zero raised to a negative power is undefined",
    "is out of range for type double precision",
    "negative substring length not allowed",
    "value overflows numeric format",
    "division by zero",
]

total_errs = 0
while True:
    try:
        line = input()
    except EOFError:
        break
    if "ERROR" in line:
        if all(line.find(ig) == -1 for ig in ignore_errs):
            print(line.replace("ERROR", colored("ERROR", 'red', attrs=['bold'])))
            total_errs += 1

print(f"Total unignored psql errors: {total_errs}")
