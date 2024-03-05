import matplotlib.pyplot as plt
from utils import savePdf, set_font_size
def plot_svp_scatter(traps):
    set_font_size(28)
    selected_traps = traps[traps.ms_id == 9]
    axs_limit = selected_traps["svp (manual)"].max() if selected_traps["svp (manual)"].max() >= selected_traps["svp (auto)"].max() else selected_traps["svp (auto)"].max()
    axs_labels = range(0, int(axs_limit) + 5, 5)
    fig = plt.figure(figsize=(10,10))

    plt.xlim(0, axs_limit + 3)
    plt.xticks(axs_labels)

    plt.ylim(0, axs_limit + 3)
    plt.yticks(axs_labels)

    plt.xlabel(r"SVP (manual) (\%)")
    plt.ylabel(r"SVP (auto) (\%)")
    plt.scatter(selected_traps["svp (manual)"], selected_traps["svp (auto)"], c ="black", marker = "x")
    savePdf("fig-svp", fig)