from tqdm import tqdm
import pandas as pd
import numpy as np

def configure(context):
	context.stage("data.spatial.zones")
	context.stage("data.osm.add_pt_variable")
	context.config("data_path")
	context.config("counties")
	context.config("region")
def execute(context):
    df = pd.read_csv("%s/population/full_population.csv" % context.config("data_path"))
    df_households = pd.read_csv("%s/population/full_households.csv" % context.config("data_path"))
    
    p_seed = pd.read_csv("%s/population/psam_p06.csv" % context.config("data_path"))
    #p_x = p_seed[p_seed["PUMA"].isin([11106,11103,11104,11105,11102,11101,6514,6511,7106,7108,7109,7110,7114,7113,6501,6515,5911,5902,5906,5917,5916,5910,5909,5913,5907,5904,5912,5905,6505,6506,6507,6502,6503,6504,6509,6508,6512,6510,6513,7101,7102,7103,7104,7105,7107,7112,7111,7115,5914,5901,5903,5918,5908,5915,3726,3705,3763,3762,3729,3724,3758,3725,3703,3704,3702,3709,3710,3711,3757,3750,3747,3706,3768,3760,3767,3765,3769,3766,3701,3748,3728,3727,3707,3761,3759,3749,3708,3719,3722,3717,3764,3731,3718,3738,3715,3716,3713,3712,3735,3756,3751,3730,3723,3720,3736,3714,3746,3721,3737,3742,3754,3755,3752,3745,3732,3739,3753,3734,3733,3740,3744,3741,3743])]
    #p_x = p_seed[p_seed["PUMA"].isin([7313,7321,7315,7305,7314,7317,7303,7312,7311,7306,7318,7308,7302,7319,7322,7310,7307,7320,7316,7309,7304,7301])] #San Diego
    
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
    df["employment"] = df["employment"].astype("category")

    #df["age"] = df["page"].astype(np.int)
    
    ##SF and LA
    conditions_ageclass = [(df["age"] < 16),
    (df["age"] >= 16) & (df["age"] <= 25),
    (df["age"] >= 26) & (df["age"] <= 35),
    (df["age"] >= 36) & (df["age"] <= 45),
    (df["age"] >= 46) & (df["age"] <= 55),
    (df["age"] >= 56) & (df["age"] <= 65),
    (df["age"] >= 66) & (df["age"] < 79),
    (df["age"] >= 80)
    ]
    ##SD
    if (len(context.config("counties")) < 3):
        conditions_ageclass = [(df["age"] < 16), 
        (df["age"] >= 16) & (df["age"] <= 25), 
        (df["age"] >= 26) & (df["age"] <= 45), 
        (df["age"] >= 46) & (df["age"] <= 65), 
        (df["age"] >= 66)]
    #SF and LA
    choices_ageclass = ['1', '2', '3','4', '5', '6', '7', '8']
    #SD
    if (len(context.config("counties")) < 3):
        choices_ageclass = ['1', '2', '3','4', '5']

    df["age_class_hts"] = np.select(conditions_ageclass, choices_ageclass, default='1')
    df["age_class_hts"] = df["age_class_hts"].astype(int)
    #SD 
    df["number_of_vehicles"] = df["VEH"]
    #SF/LA
    #df["number_of_vehicles"] = df["hhlvehic"] - 1

    # Household size
    df_size = df[["unique_housing_id"]].groupby("unique_housing_id").size().reset_index(name = "household_size")
    df = pd.merge(df, df_size)
    
    df["household_type"] = df["hhltype"]
    
    df["household_id"] = df["unique_housing_id"]

    df.loc[df["income"] == 0, "income"] = 5000.0
    
    if (context.config("region") == "la"):
        df["home_region"]=0
        df.loc[df["zone_id"].astype(np.str).str.contains('6037', regex=False), "home_region"] = 1
        df.loc[df["zone_id"].astype(np.str).str.contains('6071', regex=False), "home_region"] = 2
        df.loc[df["zone_id"].astype(np.str).str.contains('6059', regex=False), "home_region"] = 3
        df.loc[df["zone_id"].astype(np.str).str.contains('6065', regex=False), "home_region"] = 4
        df.loc[df["zone_id"].astype(np.str).str.contains('6111', regex=False), "home_region"] = 5
    elif (context.config("region") == "sf"):
        df["home_region"]=df["zone_id"].astype(np.str).str.contains('6075', regex=False)
        df.loc[df["home_region"]==True, "home_region"] = 1
        df.loc[df["home_region"]==False, "home_region"] = 0
    else:
    	raise Exception("This region name (%s) is not supported, Try one of the following [sf, la]" % context.config("region"))    

    # Clean up
    df = df[[
        "person_id", "household_id", "household_type", "household_size",
        "number_of_vehicles", "weight",
        "zone_id", "age", "sex", "employment","age_class_hts", "income", "home_region", "pt_accessible"
    ]]
    
    # remove all people outside of the study area
    df_zones = context.stage("data.spatial.zones")
    zone_ids = set(np.unique(df_zones["zone_id"]))
    df = df[df["zone_id"].isin(zone_ids)]

    return df
