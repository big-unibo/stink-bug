import csv
import pandas as pd
from datetime import datetime

def obtain_data(datasets, time_alignment = True):
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