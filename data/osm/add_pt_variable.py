import osmium as osm
import pandas as pd
import subprocess as sp
import os.path
import numpy as np
import shapely.geometry as geo
import geopandas as gpd

def configure(context):
    context.stage("data.osm.extractfacilities")
    context.stage("data.spatial.zones")
    context.stage("data.education.education_facilities")

def execute(context):
    df_facilities = context.stage("data.osm.extractfacilities")[0]
    df_facilities = df_facilities[['x','y','tagvalue','purpose']]
    df_facilities.rename(columns={'tagvalue':'type'}, inplace=True)
        
    df_education = context.stage("data.education.education_facilities")
    df_education = df_education.astype({'x': 'float64'})
    df_education = df_education.astype({'y': 'float64'})
     
    df_facilities = pd.concat([df_facilities, df_education])    

    #get pt accessibility for each census tract
    
    df_osm = context.stage("data.osm.extractfacilities")[1]
    df_zones = context.stage("data.spatial.zones").copy()
    df_stops = df_osm[(df_osm['tagkey']=='public_transport') & (df_osm['tagvalue']=='stop_position')]
    df_stops["geometry"] = [geo.Point(*xy) for xy in zip(df_stops["x"], df_stops["y"])]
    df_stops = gpd.GeoDataFrame(df_stops, crs={"init" : "epsg:4326"})
    df_stops = df_stops.to_crs({"init" : "epsg:2227"})
    pt_zones = gpd.sjoin(df_stops, df_zones, op = "within").groupby("zone_id").size().reset_index(name = "nstops")
    df_zones = pd.merge(df_zones, pt_zones, how = "left")
    df_zones["nstops"] = df_zones["nstops"].fillna(0).astype(np.int)
    df_zones["ptdensity"] = df_zones["nstops"] / (df_zones.geometry.area/1000.0/1000.0)
    has_pt_threshold = 4

    df_zones["pt_accessible"] = (df_zones["ptdensity"] > has_pt_threshold).astype(np.int)
    df_zone_pt_accessibility = df_zones[["zone_id", "pt_accessible"]].copy()
    #merge df_facilities with df_zones
    
    df_facilities["geometry"] = [geo.Point(*xy) for xy in zip(df_facilities["x"], df_facilities["y"])]
    df_facilities = gpd.GeoDataFrame(df_facilities, crs={"init" : "epsg:4326"})
    df_facilities = df_facilities.to_crs({"init" : "epsg:2227"})
    df_facilities = gpd.sjoin(df_facilities, df_zones, op = "within")
    df_facilities = df_facilities[['x','y', 'purpose', 'geometry', 'pt_accessible', 'type']]
    df_facilities = pd.DataFrame(df_facilities.drop(columns='geometry'))    
    
    return df_facilities, df_zone_pt_accessibility
    