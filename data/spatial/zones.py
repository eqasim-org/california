import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn.neighbors import KDTree

def configure(context):
    context.config("data_path")

def execute(context):
    df_zones = gpd.read_file("%s/SF_Spatial/SF_Bay_Area_cleaned.shp" % context.config("data_path"))
    df_zones.crs = {"init" : "EPSG:4326"}
    
    df_zones = df_zones.rename(columns={"GEOID" : "zone_id"})
    df_zones["zone_id"] = df_zones["zone_id"].astype(str).str[1:].astype(int)
    df_zones = df_zones.to_crs({"init" : "EPSG:2227"})
    df_zones["COUNTYFP"] = df_zones["COUNTYFP"].astype(str)
    df_zones = df_zones[df_zones['COUNTYFP'].isin(['001', '075', '085', '081', '095', '097', '013', '041', '055'])]
    
    return df_zones[["zone_id", "geometry"]]
