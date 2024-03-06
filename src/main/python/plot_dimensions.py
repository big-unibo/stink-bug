from utils import savePdf, set_font_size
import pandas as pd
import matplotlib.pyplot as plt

asse_y = "Average (SE) yearly captures" # by trap
"""
Pre-computed data for the dimension graphs
"""
graphs = [
    {
        "name": "SVP_manual",
        "x_label": "SVP (manual) (\%)",
        "y_label": asse_y,
        "data": [
            {
                "label": "[0-15)",
                "N": 234,
                "y": 361.17,
                "error": 16.74,
                "error_label": "a"
            },
            {
                "label": "[15-30)",
                "N": 67,
                "y": 488.81,
                "error": 45.4,
                "error_label": "b"
            },
            {
                "label": "[30-45)",
                "N": 28,
                "y": 579.61,
                "error": 80.98,
                "error_label": "c"
            },
        ]
    },
    {
        "name": "river_banks_and_channels",
        "x_label": "``River banks and channels''",
        "y_label": asse_y,
        "data": [
            {
                "label": "Yes",
                "N": 267,
                "y": 414.82,
                "error": 19.49,
                "error_label": "b"
            },
            {
                "label": "No",
                "N": 81,
                "y": 376.41,
                "error": 28.54,
                "error_label": "a"
            }
        ]
    },
    {
        "name": "gardens_and_groves",
        "x_label": "``Gardens and groves''",
        "y_label": asse_y,
        "data": [
            {
                "label": "Yes",
                "N": 309,
                "y": 414.35,
                "error": 17.43,
                "error_label": "b"
            },
            {
                "label": "No",
                "N": 39,
                "y": 327.95,
                "error": 41.37,
                "error_label": "a"
            }
        ]
    },
    {
        "name": "hedges_and_borders",
        "x_label": "``Hedges and borders''",
        "y_label": asse_y,
        "data": [
            {
                "label": "Yes",
                "N": 227,
                "y": 439.56,
                "error": 21.66,
                "error_label": "b"
            },
            {
                "label": "No",
                "N": 121,
                "y": 339.19,
                "error": 21.72,
                "error_label": "a"
            }
        ]
    },
    {
        "name": "buildings_season",
        "x_label": "Number of buildings",
        "y_label": asse_y,
        "data": [
            {
                "label": "0",
                "N": 16,
                "y": 403.81,
                "error": 71.11,
                "error_label": "b"
            },
            {
                "label": "1-2",
                "N": 91,
                "y": 413.59,
                "error": 29.13,
                "error_label": "b"
            },
            {
                "label": "3-4",
                "N": 122,
                "y": 343.23,
                "error": 23.5,
                "error_label": "a"
            },
            {
                "label": "$\geq$ 5",
                "N": 119,
                "y": 460.93,
                "error": 32.19,
                "error_label": "b"
            }
        ]
    },
    {
        "name": "buildings_spring",
        "x_label": "Number of buildings",
        "y_label": asse_y,
        "data": [
            {
                "label": "0",
                "N": 16,
                "y": 7.69,
                "error": 2.29,
                "error_label": "a"
            },
            {
                "label": "1-2",
                "N": 91,
                "y": 20.21,
                "error": 3.24,
                "error_label": "b"
            },
            {
                "label": "3-4",
                "N": 122,
                "y": 19.9,
                "error": 2.94,
                "error_label": "b"
            },
            {
                "label": "$\geq$ 5",
                "N": 119,
                "y": 25.81,
                "error": 2.88,
                "error_label": "b"
            }
        ]
    },
    {
        "name": "buildings_autumn",
        "x_label": "Number of buildings",
        "y_label": asse_y,
        "data": [
            {
                "label": "0",
                "N": 16,
                "y": 177.19,
                "error": 24.07,
                "error_label": "b"
            },
            {
                "label": "1-2",
                "N": 91,
                "y": 184.22,
                "error": 12.84,
                "error_label": "b"
            },
            {
                "label": "3-4",
                "N": 122,
                "y": 162.77,
                "error": 9.94,
                "error_label": "a"
            },
            {
                "label": "$\geq$ 5",
                "N": 119,
                "y": 229.18,
                "error": 16.18,
                "error_label": "c"
            }
        ]
    },
]

set_font_size(50)
def bar_plot_paper(name, data, x_label, y_label):
    """
    Bar plot for the paper
    :param name: the name of the plot
    :param data: the input data
    :param x_label: the x label
    :param y_label: the y label
    :return: the bar plot saved as pdf
    """
    plt.clf()
    fig = plt.figure(figsize = (13,13))#(10,10)
    df = pd.DataFrame.from_records([d for d in data])
    # creating the bar plot
    df["x"] = df.apply(lambda x: ("%s\n(N=%s)") % (x["label"], x["N"]), axis = 1)
    bars = plt.bar(df["x"], df['y'], yerr=df['error'], color = "silver", ecolor='black', capsize=5, width = 0.8)
    if "buildings_" not in name:
        plt.xlabel(x_label + "\n" if name == "SVP_manual" else "Trap is close to Category\n" + x_label)
    else:
        plt.xlabel(x_label)
    if name == "buildings_spring":
        plt.ylabel(y_label, labelpad=25)
    else:
        plt.ylabel(y_label)
    if "buildings_" not in name:
        max_value = 800
        steps = 100
    else:
        if name == "buildings_spring":
            steps = 10
            max_value = 40
        elif name == "buildings_autumn":
            steps = 50
            max_value = 300
        else:
            steps = 100
            max_value = 600
    if "buildings_" in name:
        season = "Spring" if name == "buildings_spring" else "Autumn" if name == "buildings_autumn" else "ALL"
        plt.title("Season=" + season, loc='right')
    error_label_height = .2*steps
    plt.ylim([0, max_value + max_value*0.00625])
    plt.yticks(range(0, int(max_value + 1), steps))

    for i in range(len(data)):
        plt.text(i, data[i]["y"] + data[i]["error"] + error_label_height, data[i]["error_label"], ha = 'center')
    savePdf("graphs/fig-dim_capts_vs_%s" % name, fig)

def plot_dim_vs():
    """
    Plot the dimensions vs dimensions graphs and save pdfs
    :return: the saved pdfs
    """
    for graph in graphs:
        bar_plot_paper(graph["name"],graph["data"],graph["x_label"],graph["y_label"])