import matplotlib.pyplot as plt
from utils import savePdf

def plot_trend_data(fact, captures_categories, captures_categories_translated_reduced):
    captures_colors = ['black', 'gray', 'lightgray']
    trend_data = fact.reset_index().groupby(['ms_id', 'week']).mean(captures_categories).reset_index()
    ms_ids = trend_data.ms_id.unique()
    plt.clf()
    fig, axs = plt.subplots(len(ms_ids), figsize=(20,5*len(ms_ids)), sharex=True)
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