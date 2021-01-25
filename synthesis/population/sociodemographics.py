import pandas as pd
import numpy as np

def configure(context):
    context.stage("synthesis.population.matching")
    context.stage("synthesis.population.sampled")
    context.stage("data.hts.cleaned")
    

def execute(context):
    df_matching, unmatched_ids = context.stage("synthesis.population.matching")
    df_persons = context.stage("synthesis.population.sampled")

    df_hts = pd.DataFrame(context.stage("data.hts.cleaned")[0], copy = True)
    df_hts["hts_person_id"] = df_hts["person_id"]
    del df_hts["person_id"]

    df_persons = df_persons[[
        "person_id", "household_id",
        "age", "sex", "employment", "number_of_vehicles",
        "census_person_id", "household_size",
        "zone_id", "income", "home_region"
    ]]

    df_hts = df_hts[[
        "hts_person_id", "has_license", "has_pt_subscription",
        "number_of_bikes",
        "is_passenger", "commute_mode", "commute_distance",
        "has_work_trip", "has_education_trip"
    ]]

    #df_income = df_income[[
    #    "household_id", "household_income"
    #]]

    assert(len(df_matching) == len(df_persons) - len(unmatched_ids))

    # Merge in attributes from HTS
    df_persons = pd.merge(df_persons, df_matching, on = "person_id", how = "inner")
    df_persons = pd.merge(df_persons, df_hts, on = "hts_person_id", how = "left")
    #df_persons = pd.merge(df_persons, df_income, on = "household_id", how = "left")

    # Reset children
    #children_selector = df_persons["age"] < c.HTS_MINIMUM_AGE
    #df_persons.loc[children_selector, "has_license"] = False
    #df_persons.loc[children_selector, "has_pt_subscription"] = False

    # Add car availability
    df_cars = df_persons[["household_id", "number_of_vehicles", "number_of_bikes", "household_size"]].drop_duplicates("household_id")
    #df_licenses = df_persons[["household_id", "has_license"]].groupby("household_id").sum().reset_index()
    #df_licenses.columns = ["household_id", "licenses"]

    df_car_availability = df_cars.copy()

    df_car_availability.loc[:, "car_availability"] = "all"
    df_car_availability.loc[df_car_availability["number_of_vehicles"] < df_car_availability[ "household_size"], "car_availability"] = "some"
    df_car_availability.loc[df_car_availability["number_of_vehicles"] == 0, "car_availability"] = "none"

    df_car_availability["car_availability"] = df_car_availability["car_availability"].astype("category")
    df_persons = pd.merge(df_persons, df_car_availability[["household_id", "car_availability"]])

    # Add bike availability
    df_persons.loc[:, "bike_availability"] = "all"
    df_persons.loc[df_persons["number_of_bikes"] < df_persons["household_size"], "bike_availability"] = "some"
    df_persons.loc[df_persons["number_of_bikes"] == 0, "bike_availability"] = "none"
    df_persons["bike_availability"] = df_persons["bike_availability"].astype("category")
    
    return df_persons
