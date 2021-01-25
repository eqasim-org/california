from tqdm import tqdm
import pandas as pd
import numpy as np
import simpledbf
import itertools

def configure(context):
    context.stage("data.spatial.zones")
    context.stage("data.od.raw")

def execute(context):
       
    df_od = context.stage("data.od.raw")
    df_od.columns = ["origin_id", "destination_id", "mode", "weight"]
    df_zones = context.stage("data.spatial.zones")
    zone_ids = set(np.unique(df_zones["zone_id"]))
    df_od = df_od[df_od["origin_id"].isin(zone_ids)]
    df_od = df_od[df_od["destination_id"].isin(zone_ids)]

    df_od_car = df_od[df_od["mode"]=='Car, truck, or van -- Drove alone'][["origin_id", "destination_id", "weight"]]
    df_od_car.columns = ["origin_id", "destination_id", "weight_car"]
    df_od_passenger = df_od[df_od["mode"]=='Car, truck, or van -- Carpooled'][["origin_id", "destination_id", "weight"]]
    df_od_passenger.columns = ["origin_id", "destination_id", "weight_passenger"]
    df_od_pt = df_od[(df_od["mode"]=='Public transportation') | (df_od["mode"]=='Taxicab, motorcycle or other method')][["origin_id", "destination_id", "weight"]]
    df_od_pt.columns = ["origin_id", "destination_id", "weight_pt"]
    df_od_bw = df_od[df_od["mode"]=='Bicycle or walked'][["origin_id", "destination_id", "weight"]]
    df_od_bw.columns = ["origin_id", "destination_id", "weight_walk"]  

    df_work_totals_car = df_od_car[["origin_id", "weight_car"]].groupby("origin_id").sum().reset_index()
    df_work_totals_car["total"] = df_work_totals_car["weight_car"]
    del df_work_totals_car["weight_car"]
    df_work_totals_passenger= df_od_passenger[["origin_id", "weight_passenger"]].groupby("origin_id").sum().reset_index()
    df_work_totals_passenger["total"] = df_work_totals_passenger["weight_passenger"]
    del df_work_totals_passenger["weight_passenger"]
    df_work_totals_pt= df_od_pt[["origin_id", "weight_pt"]].groupby("origin_id").sum().reset_index()
    df_work_totals_pt["total"] = df_work_totals_pt["weight_pt"]
    del df_work_totals_pt["weight_pt"]
    df_work_totals_walk= df_od_bw[["origin_id", "weight_walk"]].groupby("origin_id").sum().reset_index()
    df_work_totals_walk["total"] = df_work_totals_walk["weight_walk"]
    del df_work_totals_walk["weight_walk"]
    zone_ids = set(np.unique(df_od["origin_id"])) | set(np.unique(df_od["destination_id"]))

    # Impute totals
    #df_work = pd.merge(df_work, df_work_totals, on = ["origin_id", "commute_mode"])
    df_work_car = pd.merge(df_od_car, df_work_totals_car, on = "origin_id")
    df_work_passenger= pd.merge(df_od_passenger, df_work_totals_passenger, on = "origin_id")
    df_work_pt= pd.merge(df_od_pt, df_work_totals_pt, on = "origin_id")
    df_work_walk= pd.merge(df_od_bw, df_work_totals_walk, on = "origin_id")

#    df_education = pd.merge(df_education, df_education_totals, on = "origin_id")
   
    # Compute probabilities
    df_work_car["weight_car"] /= df_work_car["total"]
    df_work_passenger["weight_passenger"] /= df_work_passenger["total"]
    df_work_pt["weight_pt"] /= df_work_pt["total"]
    df_work_walk["weight_walk"] /= df_work_walk["total"]
#    df_education["weight"] /= df_education["total"]

    #assert(sum(df_work_totals["total"] == 0.0) == 0)
    #assert(sum(df_education_totals["total"] == 0.0) == 0)

    df_work_car = df_work_car.dropna()
    df_work_passenger = df_work_passenger.dropna()
    df_work_pt = df_work_pt.dropna()
    df_work_walk = df_work_walk.dropna()
#    df_education = df_education.dropna()

    # Cleanup
    df_work_car = df_work_car[["origin_id", "destination_id", "weight_car"]]
    df_work_passenger = df_work_passenger [["origin_id", "destination_id", "weight_passenger"]]
    df_work_pt = df_work_pt[["origin_id", "destination_id", "weight_pt"]]
    df_work_walk = df_work_walk[["origin_id", "destination_id", "weight_walk"]]
    
    # Fix missing zones
    existing_work_ids = set(np.unique(df_work_car["origin_id"]))
    missing_work_ids = zone_ids - existing_work_ids


    # TODO: Here we could take the zones of nearby zones in the future. Right now
    # we just distribute evenly (after all these zones don't seem to have a big impact
    # if there is nobody in the data set).

    work_rows = []
    for origin_id in missing_work_ids:
        work_rows.append((origin_id, origin_id, 1.0))
        #for destination_id in existing_work_ids:
        #    work_rows.append((origin_id, destination_id, 1.0 / len(existing_work_ids)))
    df_work_car = pd.concat([df_work_car, pd.DataFrame.from_records(work_rows, columns = ["origin_id", "destination_id", "weight_car"])])

    existing_work_ids = set(np.unique(df_work_passenger["origin_id"]))
    missing_work_ids = zone_ids - existing_work_ids


    # TODO: Here we could take the zones of nearby zones in the future. Right now
    # we just distribute evenly (after all these zones don't seem to have a big impact
    # if there is nobody in the data set).

    work_rows = []
    for origin_id in missing_work_ids:
        work_rows.append((origin_id, origin_id, 1.0))
        #for destination_id in existing_work_ids:
        #    work_rows.append((origin_id, destination_id, 1.0 / len(existing_work_ids)))
    df_work_passenger = pd.concat([df_work_passenger, pd.DataFrame.from_records(work_rows, columns = ["origin_id", "destination_id", "weight_passenger"])])

    existing_work_ids = set(np.unique(df_work_pt["origin_id"]))
    missing_work_ids = zone_ids - existing_work_ids

    # TODO: Here we could take the zones of nearby zones in the future. Right now
    # we just distribute evenly (after all these zones don't seem to have a big impact
    # if there is nobody in the data set).

    work_rows = []
    for origin_id in missing_work_ids:
        work_rows.append((origin_id, origin_id, 1.0))
        #for destination_id in existing_work_ids:
        #    work_rows.append((origin_id, destination_id, 1.0 / len(existing_work_ids)))
    df_work_pt = pd.concat([df_work_pt, pd.DataFrame.from_records(work_rows, columns = ["origin_id", "destination_id", "weight_pt"])])

    existing_work_ids = set(np.unique(df_work_walk["origin_id"]))
    missing_work_ids = zone_ids - existing_work_ids

    # TODO: Here we could take the zones of nearby zones in the future. Right now
    # we just distribute evenly (after all these zones don't seem to have a big impact
    # if there is nobody in the data set).

    work_rows = []
    for origin_id in missing_work_ids:
        work_rows.append((origin_id, origin_id, 1.0))
        #for destination_id in existing_work_ids:
        #    work_rows.append((origin_id, destination_id, 1.0 / len(existing_work_ids)))
    df_work_walk = pd.concat([df_work_walk, pd.DataFrame.from_records(work_rows, columns = ["origin_id", "destination_id", "weight_walk"])])

    df_total_car = df_work_car[["origin_id", "weight_car"]].groupby("origin_id").sum().rename({"weight_car" : "total"}, axis = 1)
    df_work_car = pd.merge(df_work_car, df_total_car, on = "origin_id")
    df_work_car["weight_car"] /= df_work_car["total"]
    del df_work_car["total"]

    df_total_passenger = df_work_passenger[["origin_id", "weight_passenger"]].groupby("origin_id").sum().rename({"weight_passenger" : "total"}, axis = 1)
    df_work_passenger= pd.merge(df_work_passenger, df_total_passenger, on = "origin_id")
    df_work_passenger["weight_passenger"] /= df_work_passenger["total"]
    del df_work_passenger["total"]
    
    df_total_pt = df_work_pt[["origin_id", "weight_pt"]].groupby("origin_id").sum().rename({"weight_pt" : "total"}, axis = 1)
    df_work_pt = pd.merge(df_work_pt , df_total_pt , on = "origin_id")
    df_work_pt["weight_pt"] /= df_work_pt["total"]
    del df_work_pt ["total"]
    
    df_total_walk = df_work_walk[["origin_id", "weight_walk"]].groupby("origin_id").sum().rename({"weight_walk" : "total"}, axis = 1)
    df_work_walk= pd.merge(df_work_walk, df_total_walk, on = "origin_id")
    df_work_walk["weight_walk"] /= df_work_walk["total"]
    del df_work_walk["total"]
    return df_work_car, df_work_pt, df_work_passenger, df_work_walk #, df_education
