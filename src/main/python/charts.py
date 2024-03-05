import pandas as pd
import numpy as np
import csv
from datetime import datetime
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from minepy import cstats

np.random.seed(3)
datasets = "../../../datasets/"
outputs = "../../../outputs/"
results = "../../../results/"

from matplotlib import rc

rc('text', usetex=True)
def set_font_size(size) :
    rc_fonts = {
        "text.usetex": True,
        "font.size": size,
        'mathtext.default': 'regular',
        'axes.titlesize': size,
        "axes.labelsize": size,
        "legend.fontsize": size,
        "xtick.labelsize": size,
        "ytick.labelsize": size,
        'figure.titlesize': size,
        'text.latex.preamble': [r'\usepackage{amsmath,amssymb,bm,physics,lmodern}'],
        "font.family": "serif",
        "font.serif": "computer modern roman",
    }
    plt.rcParams.update(rc_fonts)

def savePdf(name, fig) :
    pp = PdfPages(name + '.pdf')
    pp.savefig( fig, dpi=300, bbox_inches = "tight" )
    pp.close()
def obtain_data(time_alignment = True):
    fact = pd.read_csv(datasets + "DFM/DFM_fact_passive_monitoring.csv", quotechar='"', sep=",", quoting=csv.QUOTE_ALL)
    fact.timestamp = fact.timestamp.apply(lambda x: datetime.fromtimestamp(x))

    traps = pd.read_csv(datasets + 'DFM/DFM_dim_trap.csv', quotechar='"', sep=",", quoting=csv.QUOTE_ALL)
    dim_data = {}
    dimensions = {
        "case": "cid",
        "crop": "crop_id",
        "water_course": "water_course_id",
        "water_basin": "water_basin_id"
    }
    for dim, key in dimensions.items():
        df = pd.read_csv(datasets + 'DFM/DFM_dim_' + dim + '.csv', quotechar='"', sep=",", quoting=csv.QUOTE_ALL)
        df_bridge = pd.read_csv(datasets + 'DFM/DFM_bridge_trap_' + dim + '.csv', quotechar='"', sep=",", quoting=csv.QUOTE_ALL)
        df = pd.merge(df, df_bridge, on=key, how='left')
        dim_data[dim] = df

    fact["Tot captured"] = fact["Adults captured"] + fact["Small instars captured"] + fact["Large instars captured"]
    fact["Tot degree days"] = fact["degree_days"]
    fact["Cum degree days"] = fact["cum_degree_days"]
    def generate_season(t, ms) :
        # date in yyyy/mm/dd format
        d1 = datetime(2020, 6, 1) if ms == 6 else datetime(2021, 6, 1) if ms == 9 else datetime(2022, 6, 1)
        d2 = datetime(2020, 7, 15) if ms == 6 else datetime(2021, 7, 15) if ms == 9 else datetime(2022, 7, 15)
        d3 = datetime(2020, 9, 15) if ms == 6 else datetime(2021, 9, 15) if ms == 9 else datetime(2022, 9, 15)
        if t < d1:
            return '1-31-may'
        elif t >= d1 and t < d2:
            return '2-15-july'
        elif t >= d2 and t < d3:
            return '3-15-september'
        else:
            return '4-end'

    #remove not valid traps
    traps = traps[traps["validity"] != "invalid"]
    #fact alignment
    if time_alignment:
        fact = fact[fact["timestamp"].dt.isocalendar().week.between(18, 42)]
    traps = traps.set_index("gid").join(fact.groupby("gid").agg(monitoring = ('timestamp', 'count'))).reset_index()
    fact = fact.set_index('gid').join(traps.set_index('gid')).reset_index()
    fact["season"] = fact.apply(lambda x: generate_season(x.timestamp, x.ms_id), axis = 1)
    fact["week"] = fact["timestamp"].dt.isocalendar().week
    #remove not monitored
    fact = fact[fact["monitoring"] > 22]
    traps = traps[traps["monitoring"] > 22]
    fact=fact.sort_values(by=['gid', 'timestamp'])

    return fact, traps, dim_data

fact, traps, dim_data = obtain_data(False)

#trend dataset
captures_colors = ['black', 'gray', 'lightgray']
print(fact)
captures_categories = ["Adults captured", "Small instars captured", "Large instars captured"]
trend_data = fact.reset_index().groupby(['ms_id', 'week']).mean(captures_categories).reset_index()
ms_ids = trend_data.ms_id.unique()
plt.clf()
fig, axs = plt.subplots(len(ms_ids), figsize=(20,5*len(ms_ids)), sharex=True)
captures_categories_translated_reduced = ["Adults", "Small instars", "Large instars"]

for index, ms_id in enumerate(ms_ids):
    tmp_data = trend_data[trend_data.ms_id == ms_id].sort_values(by=['week'])
    if ms_id == 6: #cut data of 2020
        tmp_data = tmp_data[tmp_data.week >= 28]

    for i in range(0, len(captures_categories)):
        axs[index].plot(tmp_data['week'], tmp_data[captures_categories[i]], label = captures_categories_translated_reduced[i], color = captures_colors[i])
    axs[index].set_ylabel(r'Number of captures')
    axs[index].set_yticks(range(0, 50, 10))
    bbox = axs[index].get_yticklabels()[-1].get_window_extent()
    x,_ = axs[index].transAxes.inverted().transform([bbox.x0, bbox.y0])
    #graph title for each year
    axs[index].set_title(r'\textbf{2020}' if ms_id == 6 else r'\textbf{2021}' if ms_id == 9 else r'\textbf{2022}', ha='left', x=x + 0.01, y = 1)

axs[index].set_xlabel(r'Week number')

handles, labels = plt.gca().get_legend_handles_labels()
by_label = dict(zip(labels, handles))
fig.legend(by_label.values(), by_label.keys(), loc='upper right', bbox_to_anchor=(.99,1.015), ncol=3, bbox_transform=fig.transFigure)

fig.tight_layout()
plt.show(block=False)
savePdf('fig-captures', fig)

def add_week_year(x):
    iso_cal = x.isocalendar()
    return str(iso_cal[0]) + "W" + str(iso_cal[1])
def rename_area(a):
    if a == "MO-RE":
        return "West"
    if a == "BO-FE":
        return "North-east"
    if a == "FC-RA-RN":
        return "South-east"
    return a

fact['week_year'] = fact.timestamp.apply(lambda x: add_week_year(x))
compress_fact = fact[["ms_id", "area", "week_year", "Tot captured"]]
compress_fact.area = compress_fact.area.apply(lambda x: rename_area(x))
compress_fact = compress_fact[(compress_fact.area != "OTHER")]
compress_fact = compress_fact.groupby(["ms_id", "area", "week_year"]).mean("Tot captured").reset_index()
pivoted_areas = compress_fact.pivot(columns='area', values='Tot captured', index = "week_year").dropna()

areas = sorted(compress_fact.area.unique())
fig, ax = plt.subplots(1, len(areas), figsize=(24, 8.3), sharex=False, sharey=False)
i = 0
right_lim=70
for i1, a1 in enumerate(areas):
    for i2, a2 in enumerate(areas):
        if i1 > i2:
            print("Plot %s vs %s" % (a1, a2))
            ax[i].scatter(pivoted_areas[a1],pivoted_areas[a2], color = "black")
            ax[i].set_xlabel(a1)
            ax[i].set_ylabel(a2)
            ax[i].set_xlim([0,right_lim])
            ax[i].set_ylim([0,right_lim])
            ticks = [*range(0, right_lim + 15, 15)]
            ax[i].set_xticks(ticks)
            ax[i].set_yticks(ticks)

            m, b = np.polyfit(pivoted_areas[a1], pivoted_areas[a2], 1)
            ax[i].plot(pivoted_areas[a1], m*pivoted_areas[a1]+b, color = "black")
            i = i + 1
fig.tight_layout()
plt.show()
savePdf("fig-corr_areas", fig)

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


casuality_vars = captures_categories + ["Tot captured", "Tot precipitations",
                      "Avg temperature", "Max temperature", "Min temperature", "Avg humidity",
                      "Max humidity", "Min humidity",
                      "Avg wind speed", "Max wind speed", "Tot degree days", "Cum degree days"]
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
#TODO refactor and see other graphs
#fig-prec_day
#fig-wind_speed_day_avg
#not with just these data
#fig-dim_capts_vs_SVP_manual
#fig-dim_capts_vs_gardens_and_groves
#fig-dim_capts_vs_hedges_and_borders
#fig-dim_capts_vs_river_banks_and_channels
#fig-dim_capts_vs_buildings_season
#fig-dim_capts_vs_buildings_spring
#fig-dim_capts_vs_buildings_autumn
#fig-model