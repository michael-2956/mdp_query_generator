import numpy as np
from matplotlib import pyplot as plt
from numpy.polynomial.polynomial import Polynomial

def parse_grid_search_results(filename):
    nodes_per_i = {}
    with open(filename, 'r') as file:
        lines = file.readlines()
        i = None
        for line in lines:
            if line.strip().isdigit():
                i = int(line.strip())
            if "Mean number of nodes:" in line:
                nodes = float(line.split(":")[1].strip())
                nodes_per_i[i] = nodes
    return nodes_per_i

def parse_min_times_results(filename):
    min_times_per_i = {}
    with open(filename, 'r') as file:
        lines = file.readlines()
        for line in lines:
            if line.startswith('i='):
                parts = line.strip().split(', ')
                i = int(parts[0].split('=')[1])
                min_time = float(parts[1].split(': ')[1].split(' ')[0])
                min_times_per_i[i] = min_time
    return min_times_per_i

N_QUERIES_IN_TEST = 10000

def calculate_nodes_per_second(nodes_per_i, min_times_per_i):
    nodes_per_second = {}
    for i in nodes_per_i:
        if i in min_times_per_i:
            nodes_per_second[i] = nodes_per_i[i] / min_times_per_i[i] * N_QUERIES_IN_TEST
    return nodes_per_second

grid_search_path = 'experiment_results/grid_search_results.txt'
min_times_path = 'experiment_results/min_times_results.txt'

nodes_per_i = parse_grid_search_results(grid_search_path)
# min_times_per_i = parse_min_times_results(min_times_path)
min_times_per_i = {
    1: 10.2,
    2: 49.85,
    3: 60.52,
    4: 89.53,
    5: 160.38,
    6: 221.83,
    7: 287.77,
    8: 432.37,
    9: 641.94,
    10: 833.76,
    11: 1184.11,
    12: 1692.13,
}

nod_per_sec = calculate_nodes_per_second(nodes_per_i, min_times_per_i)
with open('experiment_results/nodes_per_second_results.txt', 'w') as file:
    for i in sorted(nod_per_sec.keys()):
        file.write(f"i={i}, nodes/Second: {nod_per_sec[i]:.2f}\n")

print("Nodes Per Second Speeds:")
x, y = [], []
for i in sorted(nod_per_sec.keys()):
    x.append(i)
    y.append(nod_per_sec[i])
    print(f"i={i}, Nodes/Second: {nod_per_sec[i]:.2f}")
x = np.array(x)
y = np.array(y)

plt.figure(figsize=(8, 4))
plt.plot(x, y)
plt.scatter(x, y, color='b')
plt.hlines(13367, 0, 13, color='r', linewidth=1, label="Performance with matched query size")
plt.scatter([8.901817849680889], [13367], color='r')
p = Polynomial.fit(x, np.log(y), 1)
x_line = np.linspace(x.min(), x.max(), 100)
y_line = np.exp(p(x_line))
# plt.plot(x_line, y_line, color='red', linewidth=2, label="Trend Line")
plt.xlabel(r'$l_{\mathrm{stir}}$', fontsize=14)
plt.ylabel(r'$\mathrm{nodes / sec}$', fontsize=14)
plt.xlim((0, 13))
plt.xticks(np.arange(1, 13, 1), fontsize=12)
plt.yticks(fontsize=12)
plt.ylim((0, 42000))
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig("experiment_illustrations/anticall_node_per_second.pdf", bbox_inches='tight')
# plt.show()
