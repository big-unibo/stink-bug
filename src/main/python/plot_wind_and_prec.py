import matplotlib.pyplot as plt
from utils import savePdf
import numpy as np
from utils import set_font_size

def plot_wind_and_prec(fact, casualty_vars):
    """
    Plot the wind and precipitation
    :param fact: the input fact table
    :param casualty_vars: the casualty variables
    :return: the plot of the wind and precipitation wrt the total captured instars
    """
    set_font_size(28)
    for c in ['Avg wind speed', 'Tot precipitations']:
        #scatter
        plt.clf()
        fig = plt.figure(figsize=(10,10))
        plt.xlabel("Avg wind speed (m/s)" if c == "Avg wind speed" else r"Tot precipitations (kg/m$^2$)")
        plt.ylabel(casualty_vars[3])
        add_x = 1 if c == "Avg wind speed" else 3
        plt.scatter(fact[c], fact['Tot captured'], c ="black", marker = "x")
        plt.xlim(0, fact[c].max() + add_x)
        plt.ylim(0, fact['Tot captured'].max() + 3)
        plt.xticks(np.arange(0, fact[c].max() + add_x, 2 if c == "Avg wind speed" else 25))
        savePdf("fig-%s" % c, fig)
        plt.show()