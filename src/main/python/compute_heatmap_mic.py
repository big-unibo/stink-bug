import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from minepy import cstats


def show_heatmaps(df1, df2, name):
    set_font_size(40)
    data1 = df_to_latex(df1)
    data2 = df_to_latex(df2)
    plt.clf()
    fig,(ax1,ax2, axcb) = plt.subplots(1,3,
                                       gridspec_kw={'width_ratios':[1,1,0.05]}, figsize = (45, 20))
    ax1.get_shared_y_axes().join(ax2)

    g1 = sns.heatmap(data1,cmap="YlGnBu",cbar=False,vmin=0, vmax = 1, ax=ax1)
    g1.set_ylabel('')
    g1.set_xlabel('')
    g2 = sns.heatmap(data2,cmap="YlGnBu",ax=ax2, vmin=0, vmax = 1, cbar_ax=axcb, cbar_kws=dict(label="MIC"))
    g2.set_ylabel('')
    g2.set_xlabel('')
    g2.set_yticks([])

    # may be needed to rotate the ticklabels correctly:
    for ax in [g1,g2]:
        tl = ax.get_xticklabels()
        ax.set_xticklabels(tl, rotation=90)
        tly = ax.get_yticklabels()
        ax.set_yticklabels(tly, rotation=0)
    plt.subplots_adjust(wspace=0.1)
    plt.show()
    pp = PdfPages(name + '.pdf')
    pp.savefig( fig, dpi=300, bbox_inches = "tight" )
    pp.close()

def compute_mic(data, casuality_vars):
    X1 = data.reset_index()[casuality_vars]
    X1.columns = casuality_vars
    X = X1
    X = X.transpose()
    mic_c, tic_c = cstats(X, X, alpha=9, c=5, est="mic_e")
    xs = casuality_vars
    ys = casuality_vars
    mic_c = pd.DataFrame(mic_c, index=ys, columns=xs)
    return mic_c

def aggregate_matrix(matrixs, casuality_vars):
    aggregate_matrix = pd.DataFrame(np.zeros((len(casuality_vars), len(casuality_vars))), columns=casuality_vars, index=casuality_vars)
    for c in aggregate_matrix.columns:
        for r in aggregate_matrix.index:
            arr = []
            for matrix in matrixs:
                arr.append(matrix[r][c])
            arr = [x for x in arr if str(x) != 'nan']
            aggregate_matrix.loc[r, c] = np.median(arr)
    aggregate_matrix.columns = casuality_vars
    aggregate_matrix.index = casuality_vars
    return aggregate_matrix

def plot_mic_heatmaps(fact, casuality_vars):
    mean_data = fact.reset_index().groupby("timestamp").mean(casuality_vars).sort_values(by=['timestamp'])
    mic_c = compute_mic(mean_data, casuality_vars)
    #change with old matrix values
    mic_c_np = mic_c.to_numpy()
    mic_c_new = pd.DataFrame(mic_c_np, index=casuality_vars, columns=casuality_vars)
    # compute mic for each gid
    mic_by_gid = []
    gids = fact.gid.unique()
    for gid in gids:
        gid_data = fact[fact.gid == gid].sort_values(by=['timestamp'])
        mic = compute_mic(gid_data, casuality_vars, casuality_vars)
        mic_by_gid.append(mic)
    mic_aggregate = aggregate_matrix(mic_by_gid, casuality_vars)
    show_heatmaps(mic_c_new, mic_aggregate, "fig-mic")