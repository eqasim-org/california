import gzip
from tqdm import tqdm
import pandas as pd
import numpy as np
import shapely.geometry as geo
import multiprocessing as mp

def configure(context):
    context.stage("data.od.cleaned")
    context.stage("data.spatial.zones")
    context.stage("synthesis.population.sociodemographics")
    context.stage("synthesis.population.trips")

def execute(context):
    df_persons = pd.DataFrame(context.stage("synthesis.population.sociodemographics")[["person_id", "zone_id", "census_person_id", "has_work_trip", "has_education_trip","commute_mode", "household_id"]], copy = True)
    df_persons = df_persons

    df_trips = context.stage("synthesis.population.trips")[["person_id", "destination_purpose"]]
    df_work_car_od, df_work_pt_od, df_work_passenger_od, df_work_walk_od = context.stage("data.od.cleaned")

    df_home = df_persons[["person_id", "zone_id", "household_id"]]
    #df_home.columns = ["census_person_id", "zone_id"]

    # First, home zones
    #print("Attaching home zones ...")
    #df_households = df_persons #.drop_duplicates("household_id")
    #df_households = pd.merge(df_households, df_home, on = "census_person_id", how = "left")
    #assert(not df_households["census_person_id"].isna().any())
    #df_home = df_households[["household_id", "zone_id"]]

    #df_persons = pd.merge(df_persons, df_households[["household_id", "zone_id"]], on = "household_id")
    
    # Second, work zones
    df_work = []
    for origin_id in tqdm(np.unique(df_persons["zone_id"]), desc = "Sampling work zones"):
        #do it for car
        f = (df_persons["zone_id"] == origin_id) & df_persons["has_work_trip"] & (df_persons["commute_mode"] == 'car')
        df_origin = pd.DataFrame(df_persons[f][["person_id"]], copy = True)
        df_destination = df_work_car_od[df_work_car_od["origin_id"] == origin_id]
        
        if len(df_origin) > 0:
            counts = np.random.multinomial(len(df_origin), df_destination["weight_car"].values)
            indices = np.repeat(np.arange(len(df_destination)), counts)
            df_origin.loc[:, "zone_id"] = df_destination.iloc[indices]["destination_id"].values
            df_work.append(df_origin[["person_id", "zone_id"]])

        #do it for passenger
        f = (df_persons["zone_id"] == origin_id) & df_persons["has_work_trip"] & (df_persons["commute_mode"] == 'car_passenger')
        df_origin = pd.DataFrame(df_persons[f][["person_id"]], copy = True)
        df_destination = df_work_passenger_od[df_work_passenger_od["origin_id"] == origin_id]
        
        if len(df_origin) > 0:
            counts = np.random.multinomial(len(df_origin), df_destination["weight_passenger"].values)
            indices = np.repeat(np.arange(len(df_destination)), counts)
            df_origin.loc[:, "zone_id"] = df_destination.iloc[indices]["destination_id"].values
            df_work.append(df_origin[["person_id", "zone_id"]])

        #do it for pt
        f = (df_persons["zone_id"] == origin_id) & df_persons["has_work_trip"] & (df_persons["commute_mode"] == 'pt')
        df_origin = pd.DataFrame(df_persons[f][["person_id"]], copy = True)
        df_destination = df_work_pt_od[df_work_pt_od["origin_id"] == origin_id]
        
        if len(df_origin) > 0:
            counts = np.random.multinomial(len(df_origin), df_destination["weight_pt"].values)
            indices = np.repeat(np.arange(len(df_destination)), counts)
            df_origin.loc[:, "zone_id"] = df_destination.iloc[indices]["destination_id"].values
            df_work.append(df_origin[["person_id", "zone_id"]])

        #do it for walk and bike
        f = (df_persons["zone_id"] == origin_id) & df_persons["has_work_trip"] & ((df_persons["commute_mode"] == 'walk') | (df_persons["commute_mode"] == 'bike'))
        df_origin = pd.DataFrame(df_persons[f][["person_id"]], copy = True)
        df_destination = df_work_walk_od[df_work_walk_od["origin_id"] == origin_id]
        
        if len(df_origin) > 0:
            counts = np.random.multinomial(len(df_origin), df_destination["weight_walk"].values)
            indices = np.repeat(np.arange(len(df_destination)), counts)
            df_origin.loc[:, "zone_id"] = df_destination.iloc[indices]["destination_id"].values
            df_work.append(df_origin[["person_id", "zone_id"]])

    df_work = pd.concat(df_work)

    return df_home, df_work
