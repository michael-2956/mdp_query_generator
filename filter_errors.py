from termcolor import colored

ignore_errs = [
    "a negative number raised to a non-integer power yields a complex result",
    "zero raised to a negative power is undefined",
    "is out of range for type double precision",
    "negative substring length not allowed",
    "value overflows numeric format",
    "value out of range: underflow",
    "value out of range: overflow",
    "division by zero",
    "too many grouping sets present",
]

ignored = {}

total_errs = 0
while True:
    try:
        line = input()
    except EOFError:
        break
    if "ERROR" in line:
        # if "is out of range for type double precision" in line:
        #     print(line.replace("ERROR", colored("ERROR", 'red', attrs=['bold'])))
        #     print()

        found_err = None
        for ig in ignore_errs:
            if ig in line:
                found_err = ig

        if found_err is None:
            print(line.replace("ERROR", colored("ERROR", 'red', attrs=['bold'])))
            total_errs += 1
            # print(f"{total_errs}", end="    \r", flush=True)
        else:
            # print(line.replace("ERROR", colored("ERROR", 'red', attrs=['bold'])))
            ignored.setdefault(found_err, 0)
            ignored[found_err] += 1

print(f"Total unignored psql errors: {total_errs}")
print(f"Ignored:")
for ig, num in ignored.items():
    print(f"{ig}: {num}")
