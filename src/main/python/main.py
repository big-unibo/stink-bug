import numpy as np
from get_data import obtain_data
from plot_trend_data import plot_trend_data
from plot_correlation_between_areas import plot_correlation_between_areas
from plot_svp_scatter import plot_svp_scatter
#from compute_heatmap_mic import plot_mic_heatmaps
from plot_wind_and_prec import plot_wind_and_prec

np.random.seed(3)
datasets = "../../../datasets/"
outputs = "../../../outputs/"
results = "../../../results/"

def main():
    fact, traps, dim_data = obtain_data(datasets, False)
    captures_categories = ["Adults captured", "Small instars captured", "Large instars captured"]
    captures_categories_translated_reduced = ["Adults", "Small instars", "Large instars"]
    casuality_vars = captures_categories + ["Tot captured", "Tot precipitations",
                                            "Avg temperature", "Max temperature", "Min temperature", "Avg humidity",
                                            "Max humidity", "Min humidity",
                                            "Avg wind speed", "Max wind speed", "Tot degree days", "Cum degree days"]


    print("Plot trend data")
    plot_trend_data(fact, captures_categories, captures_categories_translated_reduced)
    print("Correlation between areas")
    plot_correlation_between_areas(fact)
    print("Scatter plot SVP")
    plot_svp_scatter(traps)
    print("Compute and visualize MIC")
    # TODO plot_mic_heatmaps(fact, casuality_vars)
    print("Plot wind and precipitations")
    plot_wind_and_prec(fact, casuality_vars)
    #TODO other graphs
    #not with just these data
    #fig-dim_capts_vs_SVP_manual
    #fig-dim_capts_vs_gardens_and_groves
    #fig-dim_capts_vs_hedges_and_borders
    #fig-dim_capts_vs_river_banks_and_channels
    #fig-dim_capts_vs_buildings_season
    #fig-dim_capts_vs_buildings_spring
    #fig-dim_capts_vs_buildings_autumn
    #fig-model

if __name__ == "__main__":
    main()
