from tqdm import tqdm
import pandas as pd
import numpy as np

def configure(context):
	context.stage("data.spatial.zones")
	context.stage("data.osm.add_pt_variable")
	context.config("data_path")

def execute(context):
    df = pd.read_csv("%s/person_synthetic_merged_v2.csv" % context.config("data_path"))
    df_households = pd.read_csv("%s/household_synthetic_merged_v2.csv" % context.config("data_path"))
    df_ptaccessible_zones = context.stage("data.osm.add_pt_variable")[1]

    # Put person IDs
    df.loc[:, "person_id"] = df["unique_person_id"]
    df.loc[:, "weight"] = 1

    # Spatial
    df["zone_id"] = df["geo"].astype(np.str)
    df["geo"] = df["zone_id"] 
    
    df["zone_id"] = df["zone_id"].astype(np.int64)
    df_ptaccessible_zones["zone_id"] = df_ptaccessible_zones["zone_id"].astype(np.int64)
    #merge information if the residence zone is pt accessible
    df = pd.merge(df,df_ptaccessible_zones, on=["zone_id"], how="left")

    #join persons and households
    df_households["geo"] = df_households["geo"].astype(str)
    df_households["unique_id_in_geo"] = df_households["unique_id_in_geo"].astype(str)
    df["unique_id_in_geo"] = df["unique_id_in_geo"].astype(str)

    df = pd.merge(df,df_households,on=["geo","unique_id_in_geo"],how='left')
  
    # Attributes
    df.loc[df["pgender"] == 1, "sex"] = "male"
    df.loc[df["pgender"] == 2, "sex"] = "female"
    df["sex"] = df["sex"].astype("category")

    df["__employment"] = df["employment"]
    df["employment"] = "no"
    df.loc[df["__employment"] == 1, "employment"] = "yes"
    df.loc[df["__employment"] == 2, "employment"] = "yes"
    df.loc[df["__employment"] == 4, "employment"] = "yes"
    df.loc[df["__employment"] == 5, "employment"] = "yes"
    df.loc[df["__employment"] == 3, "employment"] = "no"
    df.loc[df["__employment"] == 6, "employment"] = "no"
    df.loc[df['school'].isna(), "employment"] = "student"
    df.loc[df["school"] == 2, "employment"] = "student"
    df.loc[df["school"] == 3, "employment"] = "student"
    
    df["employment"] = df["employment"].astype("category")

    #df["age"] = df["page"].astype(np.int)
    conditions_ageclass = [(df["age"] < 16),
    (df["age"] >= 16) & (df["age"] <= 25),
    (df["age"] >= 26) & (df["age"] <= 35),
    (df["age"] >= 36) & (df["age"] <= 45),
    (df["age"] >= 46) & (df["age"] <= 55),
    (df["age"] >= 56) & (df["age"] <= 65),
    (df["age"] >= 66) & (df["age"] < 79),
    (df["age"] >= 80)
    ]
    choices_ageclass = ['1', '2', '3','4', '5', '6','7', '8']

    df["age_class_hts"] = np.select(conditions_ageclass, choices_ageclass, default='1')
    df["age_class_hts"] = df["age_class_hts"].astype(int)
    #version ! 
   # df["number_of_vehicles"] = df["VEH"]
    #version 2
    df["number_of_vehicles"] = df["hhlvehic"] - 1

    # Household size
    df_size = df[["unique_housing_id"]].groupby("unique_housing_id").size().reset_index(name = "household_size")
    df = pd.merge(df, df_size)
    
    df["household_type"] = df["hhltype"]
    
    df["household_id"] = df["unique_housing_id"]

    df.loc[df["income"] == 0, "income"] = 5000.0
    df["sf_home"]=df["zone_id"].astype(np.str).str.contains('6075', regex=False)
    df.loc[df["sf_home"]==True, "sf_home"] = 1
    df.loc[df["sf_home"]==False, "sf_home"] = 0

    # Clean up
    df = df[[
        "person_id", "household_id", "household_type", "household_size",
        "number_of_vehicles", "weight",
        "zone_id", "age", "sex", "employment","age_class_hts", "income", "sf_home", "pt_accessible"
    ]]
    
    # remove all people outside of the study area
    df_zones = context.stage("data.spatial.zones")
    zone_ids = set(np.unique(df_zones["zone_id"]))
    df = df[df["zone_id"].isin(zone_ids)]

    return df
