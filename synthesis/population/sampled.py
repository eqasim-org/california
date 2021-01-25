import pandas as pd
import numpy as np

def configure(context):
    context.stage("data.census.cleaned")
    context.config("sampling_rate")
    
def execute(context):
    df_census = context.stage("data.census.cleaned").sort_values(by = "household_id")
    df_census["household_size"] = df_census["household_size"].astype(np.int)
    df_census["census_person_id"] = df_census["person_id"]
    assert(len(df_census["census_person_id"].unique()) == len(df_census)) 
    if context.config("sampling_rate"):
        probability = context.config("sampling_rate")
        print("Downsampling (%f)" % probability)

        household_ids = np.unique(df_census["household_id"])
        print("  Initial number of households:", len(household_ids))

        f = np.random.random(size = (len(household_ids),)) < probability
        remaining_household_ids = household_ids[f]
        print("Sampled number of households:", len(remaining_household_ids))

        df_census = df_census[df_census["household_id"].isin(remaining_household_ids)]

    return df_census
