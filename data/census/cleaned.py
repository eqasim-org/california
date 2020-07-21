from tqdm import tqdm
import pandas as pd
import numpy as np

def configure(context):
	context.stage("data.spatial.zones")
	context.stage("data.osm.add_pt_variable")
	context.config("data_path")

def execute(context):
    df = pd.read_csv("%s/population/person_synthetic.csv" % context.config("data_path"))
    df_households = pd.read_csv("%s/population/housing_synthetic.csv" % context.config("data_path"))
    
    p_seed = pd.read_csv("%s/population/psam_p06.csv" % context.config("data_path"))
    p_x = p_seed[p_seed["PUMA"].isin([7313,7321,7315,7305,7314,7317,7303,7312,7311,7306,7318,7308,7302,7319,7322,7310,7307,7320,7316,7309,7304,7301])]
    print(p_x[p_x["SCH"].isna() | (p_seed["SCH"] == 2) | (p_seed["SCH"]==3)]["PWGTP"].sum())
    print((p_x["PWGTP"].sum()))
    
    h_seed = pd.read_csv("%s/population/psam_h06.csv" % context.config("data_path"))
    p_attr = pd.DataFrame()
    p_attr["hid"] = p_seed["SERIALNO"]
    p_attr["employment"] = p_seed["ESR"]
    p_attr["school"] = p_seed["SCH"]
    p_attr["school_grade"] = p_seed["SCHG"]
    
    p_attr["pid"] = p_seed["SPORDER"]
    p_attr["age"] = p_seed["AGEP"]
    
    df = df.merge(p_attr,how="left",on=["hid","pid"])
    
    h_attr = pd.DataFrame()
    h_attr["hid"] = h_seed["SERIALNO"]

    h_attr["income"] = h_seed["HINCP"]
    # define attributes to merge
    h_attr["VEH"] = h_seed["VEH"]
    df_households = df_households.merge(h_attr,how="left",on="hid")
    
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
    print(len(df[df["employment"]=="student"]))
    print(len(df))
    exit()
    df["employment"] = df["employment"].astype("category")

    #df["age"] = df["page"].astype(np.int)
    
    ##SF and LA
    #conditions_ageclass = [(df["age"] < 16),
    #(df["age"] >= 16) & (df["age"] <= 25),
    #(df["age"] >= 26) & (df["age"] <= 35),
    #(df["age"] >= 36) & (df["age"] <= 45),
    #(df["age"] >= 46) & (df["age"] <= 55),
    #(df["age"] >= 56) & (df["age"] <= 65),
    #(df["age"] >= 66) & (df["age"] < 79),
    #(df["age"] >= 80)
    #]
    ##SD
    conditions_ageclass = [(df["age"] < 16),
    (df["age"] >= 16) & (df["age"] <= 25),
    (df["age"] >= 26) & (df["age"] <= 45),
    (df["age"] >= 46) & (df["age"] <= 65),
    (df["age"] >= 66)
    ]
    #SF and LA
    #choices_ageclass = ['1', '2', '3','4', '5', '6', '7', '8']
    #SD
    choices_ageclass = ['1', '2', '3','4', '5']

    df["age_class_hts"] = np.select(conditions_ageclass, choices_ageclass, default='1')
    df["age_class_hts"] = df["age_class_hts"].astype(int)
    #version 1 
    df["number_of_vehicles"] = df["VEH"]
    #version 2
    #df["number_of_vehicles"] = df["hhlvehic"] - 1

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
