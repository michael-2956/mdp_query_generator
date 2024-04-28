def generate_latex_table(x, y):
    # Split the data into two parts for two rows
    x1, x2 = x[:6], x[6:]
    y1, y2 = y[:6], y[6:]

    # Convert numerical data to LaTeX formatted strings
    x1_str = ' & '.join([f"${xi}$" for xi in x1])
    y1_str = ' & '.join([f"{yi:.4f}" for yi in y1])
    x2_str = ' & '.join([f"${xi}$" for xi in x2])
    y2_str = ' & '.join([f"{yi:.4f}" for yi in y2])

    # Assemble the LaTeX table
    latex_table = f"""
\\begin{{table}}[tb]
\\centering
\\begin{{tabularx}}{{\\textwidth}}{{|c|*{{6}}{{>{{\\centering\\arraybackslash}}X|}}}}
\\toprule
$l^i_{{\\text{{stir}}}}$ & {x1_str} \\\\
\\midrule
$N^i_{{\\text{{stir}}}}$ & {y1_str} \\\\
\\midrule
$l^i_{{\\text{{stir}}}}$ & {x2_str} \\\\
\\midrule
$N^i_{{\\text{{stir}}}}$ & {y2_str} \\\\
\\bottomrule
\\end{{tabularx}}
\\caption{{Mean number of nodes for each $l_{{\\text{{stir}}}}$}}
\\label{{tab:mean_tok_per_level}}
\\end{{table}}
"""
    print(latex_table)

# Example usage
x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
y = [41.1564, 99.1472, 136.8255, 179.7125, 266.3282, 360.0894, 466.9911, 629.3852, 848.3527, 1108.8857, 1484.5629, 1930.1822]
generate_latex_table(x, y)


from matplotlib import pyplot as plt
from scipy.optimize import curve_fit
import numpy as np

def model_func(x, esa, esb, s):
    return s * (1/(1-esa) + (1/(esa-1) + esa/(1 - esb)) * (esa ** (x-1)))

# (a, b, c), _ = curve_fit(model_func, x, y)
(esa, esb, s), _ = curve_fit(
    model_func, x, y,
    p0=(1.1, 0.4, 1),
    bounds=([1.00000001, 0.00000001, 0], [np.inf, 0.99999999, np.inf])
)

x_plot = np.linspace(min(x), max(x), 400)
y_plot = model_func(x_plot, esa, esb, s)

plt.figure(figsize=(8, 4))
# plt.yscale('log')
plt.scatter(x, y, color='b')
plt.plot(x_plot, y_plot, color='red', label=f'$s \cdot \mathbb{{E}}(s_{{total}})\; for\; s={s:.2f},\; \mathbb{{E}}(s_A)={esa:.2f}\; and\; \mathbb{{E}}(s_B)={esb:.2f}$')
plt.xlabel(r'$l_{\mathrm{stir}}$', fontsize=14)
plt.ylabel(r'$N_{\mathrm{stir}}$', fontsize=14)
plt.xlim((0, 13))
plt.xticks(np.arange(1, 13, 1), fontsize=12)
plt.yticks(fontsize=12)
plt.ylim((0, 2000))
plt.grid(True)
plt.legend(fontsize=12)
plt.tight_layout()
plt.savefig("experiment_illustrations/anticall_size_per_setting.pdf", bbox_inches='tight')

