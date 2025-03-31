# mode = "ours"
mode = "sqlsmith"

if mode == "ours":
    filename = "experiment_results/our_10K_balanced.txt"
    # filename = "generated_queries.sql"
    filename_pr, filename_post = filename[:-4], filename[-4:]

    inp = open(filename, "r").read()
    print(inp.count(";\n\n\n"))
    inp = "EXPLAIN " + inp.replace(";\n\n\n", ";\n\n\nEXPLAIN ")[1:]
    open(f"{filename_pr}_explain{filename_post}", "w").write(inp)
elif mode == "sqlsmith":
    # filename = "experiment_results/sqlsmith_10K.txt"
    filename = "experiment_results/sqlsmith_tpcds_10K.txt"
    filename_pr, filename_post = filename[:-4], filename[-4:]
    inp = open(filename, "r").read()
    print(inp.count(";\n"))
    inp = "EXPLAIN " + inp.replace(";\n", ";\nEXPLAIN ")[:-len("EXPLAIN ")]
    open(f"{filename_pr}_explain{filename_post}", "w").write(inp)
