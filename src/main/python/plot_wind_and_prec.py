import matplotlib.pyplot as plt
import numpy as np
from utils import savePdf, set_font_size
from pyloess import loess

def plot_wind_and_prec(fact, casualty_vars):
    """
    Plot the wind and precipitation
    :param fact: the input fact table
    :param casualty_vars: the casualty variables
    :return: the plot of the wind and precipitation wrt the total captured instars
    """
    set_font_size(28)
    for c in ['Avg wind speed', 'Tot precipitations']:
        # Filter out NaN values
        valid_indices = ~np.isnan(fact[c]) & ~np.isnan(fact['Tot captured'])
        fact_filtered = fact[valid_indices].reset_index()
        # Create a copy of the filtered DataFrame without the index
        fact_filtered.sort_values(by=c)
        # Extract x and y values from the filtered DataFrame
        x = fact_filtered[c]
        y = fact_filtered['Tot captured']
        #scatter
        plt.clf()
        fig = plt.figure(figsize=(10,10))
        plt.xlabel("Avg wind speed (m/s)" if c == "Avg wind speed" else r"Tot precipitations (kg/m$^2$)")
        plt.ylabel(casualty_vars[3])
        add_x = 1 if c == "Avg wind speed" else 3
        plt.scatter(x, y, c ="black", marker = "x")
        plt.xlim(0, x.max() + add_x)
        plt.ylim(0, y.max() + 3)
        plt.xticks(np.arange(0, x.max() + add_x, 2 if c == "Avg wind speed" else 25))
        # Perform LOESS regression
        x_np = x.to_numpy()
        y_np = y.to_numpy()
        eval_x =  np.array([elem / 10 for elem in np.linspace(x.min() * 10, x.max() * 10, 10)])
        smoothed_y = loess(x_np, y_np, eval_x=eval_x, span=0.33, degree=2)
        plt.plot(eval_x, smoothed_y, color='red', label='LOESS')
        plt.legend()
        savePdf("graphs/fig-%s" % c, fig)