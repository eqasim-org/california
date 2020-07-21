import numpy as np
import pandas as pd

def configure(context):
    context.stage("data.hts.cleaned")

def calculate_bounds(values, bin_size):
    values = np.sort(values)

    bounds = []
    current_count = 0
    previous_bound = None

    for value in values:
        if value == previous_bound:
            continue

        if current_count < bin_size:
            current_count += 1
        else:
            current_count = 0
            bounds.append(value)
            previous_bound = value

    bounds[-1] = np.inf
    return bounds

def execute(context):
    # Prepare data
    df_persons, df_trips = context.stage("data.hts.cleaned")
    df_trips = pd.merge(df_trips, df_persons[["person_id", "weight"]]#.rename(columns = { "person_weight": "weight" })
)
    df_trips["travel_time"] = df_trips["arrival_time"] - df_trips["departure_time"]
    

    distance_column = "crowfly_distance"
    df = df_trips[["mode", "travel_time", distance_column, "weight", "origin_purpose", "destination_purpose"]].rename(columns = { distance_column: "distance" })

    # Filtering
    primary_activities = ["home", "work", "education"]
    df = df[~(
        df["origin_purpose"].isin(primary_activities) &
        df["destination_purpose"].isin(primary_activities)
    )]

    # Calculate distributions
    modes = df["mode"].unique()

    bin_size = 100
    distributions = {}

    for mode in modes:
        # First calculate bounds by unique values
        f_mode = df["mode"] == mode

        bounds = calculate_bounds(df[f_mode]["travel_time"].values, bin_size)

        distributions[mode] = dict(bounds = np.array(bounds), distributions = [])

        # Second, calculate distribution per band
        for lower_bound, upper_bound in zip([-np.inf] + bounds[:-1], bounds):
            f_bound = (df["travel_time"] > lower_bound) & (df["travel_time"] <= upper_bound)

            # Set up distribution
            values = df[f_mode & f_bound]["distance"].values
            weights = df[f_mode & f_bound]["weight"].values

            sorter = np.argsort(values)
            values = values[sorter]
            weights = weights[sorter]

            cdf = np.cumsum(weights)
            cdf /= cdf[-1]

            # Write distribution
            distributions[mode]["distributions"].append(dict(cdf = cdf, values = values, weights = weights))

    return distributions