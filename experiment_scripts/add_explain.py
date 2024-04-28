filename = "experiment_results/our_100K_balanced.txt"
# filename = "generated_queries.sql"
filename_pr, filaname_post = filename[:-4], filename[-4:]

inp = open(filename, "r").read()
print(inp.count(";\n\n\nSELECT"))
inp = "EXPLAIN " + inp.replace(";\n\n\nSELECT", ";\n\n\nEXPLAIN SELECT")[1:]
open(f"{filename_pr}_explain{filaname_post}", "w").write(inp)
