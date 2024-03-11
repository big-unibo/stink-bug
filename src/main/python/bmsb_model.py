from datetime import datetime
from collections import namedtuple
from scipy.optimize import curve_fit
from sklearn.metrics import mean_squared_error
import numpy as np
import matplotlib.pyplot as plt
from utils import savePdf, set_font_size

VarKey = namedtuple("VarKey", "ms_id gen p var".split(" "))
SummKey = namedtuple("SummKey", "gen p var".split(" "))
SummKeyYear = namedtuple("SummKeyYear", "gen p".split(" "))

def legend_without_duplicate_labels(fig):
    """
    This function is used to remove duplicate labels from the legend.
    :param fig: the input figure
    :return: the figure without duplicate labels in the legend
    """
    handles, labels = plt.gca().get_legend_handles_labels()
    labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: t[0]))
    by_label = dict(zip(labels, handles))
    fig.legend(by_label.values(), by_label.keys(), loc='upper right', bbox_to_anchor=(.99,1.015), ncol=len(labels), bbox_transform=fig.transFigure)

def model(x, alfa, beta, gamma):
    """
    This function is used to generate the model.
    :param x:  the input data
    :param alfa: the alfa parameter of the logistic function
    :param beta: the beta parameter of the logistic function
    :param gamma: the gamma parameter of the logistic function
    :return: the model
    """
    return alfa / (1+np.exp(-(x-gamma)/beta)) # + err (standard error)

def fit_model(x, y):
    """
    This function is used to fit the model.
    :param x: the x input data
    :param y: the y input data
    :return: the values of the model
    """
    return curve_fit(model, x, y, p0 = (0, 100, x.max() - x.min()), maxfev=5000)#p0=(y.max(), y.min(), x.max() - x.min()))

def navigate_data_dict(data_dict, captures_categories):
    """
    This function is used to navigate the data dictionary.
    :param data_dict: the input data dictionary
    :param captures_categories: the input captures categories
    :return: the summary dictionary and plot the model results
    """
    rmses = []
    rmses_filtered = []
    summary_dict = {}
    for k, v in data_dict.items():
        if len(v) > 1 and k.ms_id == 9:
            #generate a model
            x_2021 = v["cum_degree_days"].to_numpy()
            y_2021 = v["gen_tot_cum_perc"].to_numpy()
            summary_key_dict = {}
            summary_key_dict['x_2021'] = x_2021
            summary_key_dict['y_2021'] = y_2021
            summary_key = SummKeyYear(k.gen, k.p)

            #training years
            other_ks = [VarKey(ms_id=6, gen=k.gen, p=k.p, var = 6),
                        VarKey(ms_id=12, gen=k.gen, p=k.p, var = 12)]
            x, y = x_2021, y_2021

            other_data_is_present = False
            for other_k in other_ks:
                if other_k in data_dict:
                    other_data = data_dict[other_k]
                    x_ms = other_data["cum_degree_days"].to_numpy()
                    y_ms = other_data["gen_tot_cum_perc"].to_numpy()
                    #il modello è trainato su tutto
                    x, y = zip(*sorted(zip(np.concatenate((x, x_ms), axis=None), np.concatenate((y, y_ms), axis=None))))
                    other_data_is_present = True
                else :
                    x_ms = []
                    y_ms = []
                k_year = 2020 if other_k.ms_id == 6 else 2022 if other_k.ms_id == 12 else other_k.ms_id
                summary_key_dict['x_%s' % (k_year)] = x_ms
                summary_key_dict['y_%s' % (k_year)] = y_ms

            x = np.asarray(x)
            y = np.asarray(y)
            summary_key_dict['x'] = x
            summary_key_dict['y'] = y

            if len(x) > 0:
                popt, pcov = fit_model(x, y)
                summary_key_dict['model'] = popt

                y_test = model(x, *popt)
                summary_key_dict['y_test'] = y_test
                rmse = mean_squared_error(y, y_test, squared=False)
                if other_data_is_present == False:
                    rmses_filtered.append(rmse)
                rmses.append(rmse)
                summary_dict[summary_key] = summary_key_dict
    plot_summary(summary_dict.items(), captures_categories)
    return summary_dict

"""
Colours for the plot
"""
years= {
    2022: {
        "marker": 's',
        "facecolors":'black'
    },
    2021: {
        "marker": 'o',
        "facecolors":'none'
    },
    2020: {
        "marker":'o',
        "facecolors":'black'
    }
}

def plot_summary(items, captures_categories):
    """
    This function is used to plot the summary.
    :param items: the input items where applied the model
    :param captures_categories: the input captures categories
    :return: the plot of the model results
    """
    plt.clf()
    fig, axs = plt.subplots(len(captures_categories), figsize=(25,6*len(captures_categories)))

    for k,v in items:
        index = captures_categories.index(k.gen)
        #Model plot
        axs[index].plot(v['x'], v['y_test'], label = "Model", color = 'black')
        for year in years:
            year_x = 'x_%s' %(year)
            year_y = 'y_%s' %(year)
            if len(v[year_x]) > 0:
                #scatter plot of the data
                axs[index].scatter(v[year_x], v[year_y], label = year, color = 'black', marker = years[year]["marker"],  facecolors = years[year]["facecolors"])
        axs[index].set_xlabel('Cumulative degree days')
        axs[index].set_ylabel(r'Cumulative captures \%')
        bbox = axs[index].get_yticklabels()[-1].get_window_extent()
        x,_ = axs[index].transAxes.inverted().transform([bbox.x0, bbox.y0])
        #Graph title for each generation
        axs[index].set_title(captures_categories[captures_categories.index(k.gen)], ha='left', x=x + 0.01, y = 1.1)
        axs[index].set_ylim([0, 100])

    legend_without_duplicate_labels(fig)
    fig.tight_layout()
    savePdf('graphs/fig-model', fig)

year_dates = {
    6: {
        "Adults captured": {
            2: {
                6: [datetime(2020, 7, 5), datetime(2020, 8, 31)]
            },
            3: {
                6: [datetime(2020, 8, 31), datetime(2020, 10, 12)]
            }
        },
        "Small instars captured": {
            2: {
                6: [datetime(2020, 7, 13), datetime(2020, 10, 12)]
            }
        },
        "Large instars captured": {
            1: {
                6: [datetime(2020, 4, 26), datetime(2020, 8, 10)]
            },
            2: {
                6: [datetime(2020, 8, 10), datetime(2020, 10, 12)]
            }
        }
    },
    9: {
        "Adults captured": {
            1: {
                9: [datetime(2021, 3, 7), datetime(2021, 7, 5)]
            },
            2: {
                9: [datetime(2021, 7, 5), datetime(2021, 8, 30)]
            },
            3: {
                9: [datetime(2021, 8, 30), datetime(2021, 10, 11)]
            }
        },
        "Small instars captured" : {
            1: {
                9: [datetime(2021, 3, 7), datetime(2021, 7, 19)]
            },
            2: {
                9: [datetime(2021, 7, 19), datetime(2021, 10, 11)]
            }
        },
        "Large instars captured" : {
            1: {
                9: [datetime(2021, 3, 7), datetime(2021, 8, 2)]
            },
            2: {
                9: [datetime(2021, 8, 2), datetime(2021, 10, 11)]
            }
        }
    },
    12: {
        "Adults captured": {
            1: {
                12: [datetime(2022, 3, 28), datetime(2022, 6, 27)]
            },
            2: {
                12: [datetime(2022, 6, 27), datetime(2022, 8, 15)]
            },
            3: {
                12: [datetime(2022, 8, 15), datetime(2022, 10, 17)]
            }
        },
        "Small instars captured" : {
            1: {
                12: [datetime(2022, 3, 28), datetime(2022, 7, 18)]
            },
            2: {
                12: [datetime(2022, 7, 18), datetime(2022, 10, 17)]
            }
        },
        "Large instars captured" : {
            1: {
                12: [datetime(2022, 3, 28), datetime(2022, 8, 1)]
            },
            2: {
                12: [datetime(2022, 8, 1), datetime(2022, 10, 17)]
            }
        }
    }
}

def create_dataset_divided(fact, vars_dict, dates_dict, var_name, captures_categories):
    """
    This function is used to create the dataset divided for each capture category and year.
    :param fact: the input fact
    :param vars_dict: the input variables dictionary, for split the data by var_name
    :param dates_dict: the input dictionary for split the data considering the peak dates
    :param var_name: the variable name to consider for the split
    :param captures_categories: the capture categories
    :return: the dictionary of the dataset divided
    """
    fact['week'] = fact['timestamp'].apply(lambda x: x.week)
    res_dict = {}
    for ms_id in fact.ms_id.unique():
        ms_id_fact = fact[fact.ms_id == ms_id]
        if ms_id == 6: #cut data of 2020
            ms_id_fact = ms_id_fact[ms_id_fact.week >= 28]

        for gen in captures_categories:
            #definizione dei picchi
            tot_p = 2
            if gen == 'Adults captured' and ms_id != 6:
                tot_p = 3
            if gen == "Small instars captured" and ms_id == 6:
                tot_p = 1
            if ms_id == 2019:
                tot_p = 1
            p = 0
            while p < tot_p :
                p = p + 1
                actual_p = p
                used_data = ms_id_fact

                if ms_id == 6 and (gen == 'Adults captured' or gen == "Small instars captured"):
                    actual_p = p + 1

                for v in vars_dict[ms_id]:
                    var_data = used_data[used_data[var_name] == v]
                    p_dates = dates_dict[ms_id][gen][actual_p][v]
                    var_data = var_data[(var_data.timestamp > p_dates[0]) & (var_data.timestamp <= p_dates[1])]
                    var_key = VarKey(ms_id=ms_id, gen=gen, p=actual_p, var = v)
                    res_dict[var_key] = get_dataframe(var_data, gen)

    return res_dict

def get_grouped_timestamp_value(x) :
    """
    This function is used to get the grouped timestamp value.
    :param x: the input value (date)
    :return: a string representing the grouped timestamp value considering year and week
    """
    if x.week < 10:
        return "% s0% s" % (x.year, x.week)
    else :
        return "% s% s" % (x.year, x.week)
def get_dataframe(df, gen):
    """
    This function is used to get the dataframe.
    :param df: the input dataframe
    :param gen: the input generation to consider
    :return: a dataframe with the grouped data
    """
    aggregate_field_name = 'year_week'
    df[aggregate_field_name] = df['timestamp'].apply(lambda x: get_grouped_timestamp_value(x))
    new_df = df.reset_index()[[gen, 'cum_degree_days', 'ms_id', 'week', aggregate_field_name]]
    new_df = new_df.groupby(aggregate_field_name).agg(cum_degree_days=('cum_degree_days', 'mean'), gen_tot=(gen, 'mean'), ms_id=('ms_id', 'min'), week=('week', 'min'))
    new_df = new_df.sort_values(by=[aggregate_field_name])
    new_df['gen_tot_cum'] = (new_df['gen_tot']).groupby(new_df['ms_id']).cumsum()
    new_df['gen_tot_cum_perc'] = ( new_df['gen_tot_cum'] /  new_df['gen_tot_cum'].max()) * 100
    return new_df.reset_index()[['gen_tot_cum_perc', 'cum_degree_days']]
def generate_and_plot_model(fact, captures_categories):
    """
    This function is used to generate and plot the model.
    :param fact: the input fact
    :param captures_categories: the input capture categories
    :return: plot the model results
    """
    set_font_size(40)
    year_dict = create_dataset_divided(fact, {
        6: [6],
        9: [9],
        12: [12]
    }, year_dates, 'ms_id', captures_categories)

    year_summary = navigate_data_dict(year_dict, captures_categories)