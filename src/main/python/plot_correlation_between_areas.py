import matplotlib.pyplot as plt
from utils import savePdf, set_font_size
import numpy as np

def add_week_year(x):
    """
    Add week year to the timestamp
    :param x: the timestamp
    :return: the timestamp with the week year
    """
    iso_cal = x.isocalendar()
    return str(iso_cal[0]) + "W" + str(iso_cal[1])
def rename_area(a):
    """
    Rename the area
    :param a: the input area
    :return: the renamed area
    """
    if a == "MO-RE":
        return "West"
    if a == "BO-FE":
        return "North-east"
    if a == "FC-RA-RN":
        return "South-east"
    return a

def plot_correlation_between_areas(fact):
    """
    Plot the correlation between areas
    :param fact: the input fact table
    :return: the plot of the correlation between areas
    """
    set_font_size(28)
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