import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from minepy import cstats
from utils import set_font_size

def df_to_latex(df):
    """
    Replace _ with \_ in the column and index names
    :param df: the input dataframe
    :return: the dataframe with columns and indexes _ replaced by \_
    """
    data = df
    data.columns = df.columns.str.replace('_', '\_')
    data.index = df.index.str.replace('_', '\_')
    return data

def show_heatmaps(df1, df2, name):
    """
    Show two heatmaps side by side
    :param df1: a dataframe
    :param df2: a dataframe
    :param name: the name of the pdf file
    :return: the pdf file with the two heatmaps
    """
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

def compute_mic(data, casualty_var):
    """
    Compute the MIC matrix for the given data
    :param data: input data
    :param casualty_var: casualty variables to consider
    :return: the MIC matrix
    """
    X1 = data.reset_index()[casualty_var]
    X1.columns = casualty_var
    X = X1
    X = X.transpose()
    mic_c, tic_c = cstats(X, X, alpha=9, c=5, est="mic_e")
    xs = casualty_var
    ys = casualty_var
    mic_c = pd.DataFrame(mic_c, index=ys, columns=xs)
    return mic_c

def aggregate_matrix(matrixs, casualty_var):
    """
    Aggregate the matrixes
    :param matrixs: input matrixes
    :param casualty_var: the casualty variables used in the matrixes
    :return: the aggregated matrix, using the median
    """
    aggregate_matrix = pd.DataFrame(np.zeros((len(casualty_var), len(casualty_var))), columns=casualty_var, index=casualty_var)
    for c in aggregate_matrix.columns:
        for r in aggregate_matrix.index:
            arr = []
            for matrix in matrixs:
                arr.append(matrix[r][c])
            arr = [x for x in arr if str(x) != 'nan']
            aggregate_matrix.loc[r, c] = np.median(arr)
    aggregate_matrix.columns = casualty_var
    aggregate_matrix.index = casualty_var
    return aggregate_matrix

def plot_mic_heatmaps(fact, casualty_vars):
    """
    Plot the MIC heatmaps
    :param fact: the input fact
    :param casualty_vars: the casualty variables
    :return: the plot the heatmaps
    """
    set_font_size(40)
    mean_data = fact.reset_index().groupby("timestamp").mean(casualty_vars).sort_values(by=['timestamp'])
    mic_c = compute_mic(mean_data, casualty_vars)
    #change with old matrix values
    mic_c_np = mic_c.to_numpy()
    mic_c_new = pd.DataFrame(mic_c_np, index=casualty_vars, columns=casualty_vars)
    # compute mic for each gid
    mic_by_gid = []
    gids = fact.gid.unique()
    for gid in gids:
        gid_data = fact[fact.gid == gid].sort_values(by=['timestamp'])
        mic = compute_mic(gid_data, casualty_vars)
        mic_by_gid.append(mic)
    mic_aggregate = aggregate_matrix(mic_by_gid, casualty_vars)
    show_heatmaps(mic_c_new, mic_aggregate, "fig-mic")