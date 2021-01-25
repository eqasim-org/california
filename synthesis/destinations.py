import gzip
from tqdm import tqdm
import pandas as pd
import numpy as np
from sklearn.neighbors import KDTree
import numpy.linalg as la
import data.spatial.utils

def configure(context):
    context.stage("data.spatial.zones")
    context.stage("data.osm.add_pt_variable")

def execute(context):

    df_zones = context.stage("data.spatial.zones")
    zone_ids = set(np.unique(df_zones["zone_id"]))

    df_opportunities = context.stage("data.osm.add_pt_variable")[0]
    df_opportunities["location_id"] = np.arange(len(df_opportunities))
    df_opportunities["offers_work"] =  df_opportunities["purpose"].str.contains("work") | df_opportunities["purpose"].str.contains("education")    
    df_opportunities["offers_other"] = True
    df_opportunities["offers_business"] = True
    df_opportunities["offers_leisure"] = df_opportunities["purpose"].str.contains("leisure")
    df_opportunities["offers_shop"] = df_opportunities["purpose"].str.contains("shop")
    df_opportunities["offers_education"] = df_opportunities["purpose"].str.contains("education")
    df_opportunities["offers_home"] = df_opportunities["purpose"].str.contains("home")
    df_opportunities["ptAccessible"] = df_opportunities['pt_accessible']
    
    df_zones = context.stage("data.spatial.zones")

    df_opportunities = data.spatial.utils.to_gpd(df_opportunities, crs = {"init" : "EPSG:4326"}).to_crs({"init" : "EPSG:2227"})
    df_opportunities["x"] = df_opportunities["geometry"].x
    df_opportunities["y"] = df_opportunities["geometry"].y
    df_opportunities = data.spatial.utils.impute(df_opportunities, df_zones, "location_id", "zone_id", fix_by_distance = False).dropna()

    existing_home_ids = set(np.unique(df_opportunities[df_opportunities["offers_home"]]["zone_id"]))
    missing_home_ids = zone_ids - existing_home_ids
  
    #assign home to centroids only for the missing zone ids
    df_centroids = df_zones[df_zones["zone_id"].isin(missing_home_ids)].copy()
    df_centroids["x"] = df_centroids["geometry"].centroid.x
    df_centroids["y"] = df_centroids["geometry"].centroid.y
    df_centroids["offers_work"] = False
    df_centroids["offers_education"] = False
    df_centroids["offers_other"] = False
    df_centroids["offers_business"] = False
    df_centroids["offers_leisure"] = False
    df_centroids["offers_shop"] = False
    df_centroids["offers_home"] = True
    df_centroids["location_id"] = np.arange(len(df_centroids)) + len(df_centroids) + int(1e6) + int(1e5)
    df_centroids["purpose"] = "home"
    df_centroids["ptAccessible"] = False
    df_opportunities = pd.concat([df_opportunities, df_centroids], sort = True)

    existing_work_ids = set(np.unique(df_opportunities[df_opportunities["offers_work"]]["zone_id"]))
    missing_work_ids = zone_ids - existing_work_ids
    df_centroids = df_zones[df_zones["zone_id"].isin(missing_work_ids)].copy()
    df_centroids["x"] = df_centroids["geometry"].centroid.x
    df_centroids["y"] = df_centroids["geometry"].centroid.y
    df_centroids["offers_work"] = True
    df_centroids["offers_education"] = False
    df_centroids["offers_other"] = False
    df_centroids["offers_business"] = False
    df_centroids["offers_leisure"] = False
    df_centroids["offers_shop"] = False
    df_centroids["offers_home"] = False
    df_centroids["location_id"] = np.arange(len(df_centroids)) + len(df_centroids) + int(1e6) + int(1e5)
    df_centroids["purpose"] = "work"
    df_centroids["ptAccessible"] = False
    df_opportunities = pd.concat([df_opportunities, df_centroids], sort = True)
    
    df_opportunities["location_id"] = np.arange(len(df_opportunities))
    
    df_education_types = df_opportunities[df_opportunities["purpose"].str.contains("education")]

    return df_opportunities, df_education_types
