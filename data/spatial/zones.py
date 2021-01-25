import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn.neighbors import KDTree

def configure(context):
    context.config("data_path")
    context.config("zones")
    context.config("spatial_file")

def execute(context):
    df_zones = gpd.read_file("%s/Spatial/%s" % (context.config("data_path"), context.config("spatial_file")))
    #df_zones = gpd.read_file("%s/Spatial/SanDiego.shp" % context.config("data_path"))
    #df_zones.crs = {"init" : "EPSG:4326"}
    df_zones = df_zones.rename(columns={"GEOID" : "zone_id"})
    df_zones["zone_id"] = df_zones["zone_id"].astype(str).str[1:].astype(int)
    df_zones = df_zones.to_crs({"init" : "EPSG:2227"})
    
    df_zones["COUNTYFP"] = df_zones["COUNTYFP"].astype(str)
    df_zones = df_zones[df_zones['COUNTYFP'].isin(context.config("zones"))]
    #df_zones = df_zones[df_zones['COUNTYFP'].isin(['073'])]
    
    return df_zones[["zone_id", "geometry"]]
