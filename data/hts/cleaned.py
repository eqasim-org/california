from tqdm import tqdm
import pandas as pd
import numpy as np

def configure(context):
    context.stage("data.osm.add_pt_variable")
    context.config("data_path")
    context.config("counties")
    
def execute(context):
	
    #we should make this configurable
    counties = context.config("counties")#[1.0, 13.0, 41.0, 55.0, 75.0, 81.0, 85.0, 95.0, 97.0]
    #counties = [73.0]
    daysToKeep = ['Tuesday', 'Wednesday', 'Thursday']

    ##we are defining priorties for each mode for mode imputation for trips with transfers
    ##this is necessary for later travel mode comparison 
    priorities = {
    1 : 0,
    2 : 0,
    3 : 0,
    4 : 0,
    5 : 2,
    6 : 2,
    7 : 2,
    8 : 2,
    9 : 1,
    10 : 1,
    11 : 1,
    12 : 1,
    13 : 1,
    14 : 1,
    15 : 1,
    16 : 1,
    17 : 1,
    18 : 1,
    19 : 1,
    20 : 1,
    21 : 1,
    22 : 1,
    23 : 1,
    24 : 1,
    25 : 1,
    26 : 1,
    27 : 1,
    28 : 1,
    29 : 1,
    99 : 0    
    }

    activity_type = {
    1 : 'home',
    2 : 'home',
    3 : 'home',
    4 : 'leisure',
    5 : 'home',
    6 : 'home',
    7 : 'other',
    8 : 'home',
    9 : 'work',
    10 : 'leisure',
    11 : 'work',
    12 : 'leisure',
    13 : 'leisure',
    14 : 'leisure',
    15 : 'leisure',
    16 : 'work',
    17 : 'education',
    18 : 'education',
    19 : 'leisure',
    20 : 'leisure',
    21 : 'transfer', #this one does not exist after merging, but it is here for reference
    22 : 'other', #pick up/dropp off
    23 : 'leisure',
    24 : 'leisure',
    25 : 'business',
    26 : 'other', #errands
    27 : 'shop',
    28 : 'shop',
    29 : 'other', #errands,
    30 : 'other', #errands,
    31 : 'leisure',
    32 : 'other', #errands,
    33 : 'leisure',
    34 : 'leisure',
    35 : 'leisure',
    36 : 'leisure',
    37 : 'leisure',
    38 : 'other',
    39 : 'other',
    99 : 'other'    
    }

    mode_type = {
    1 : 'walk',
    2 : 'bike',
    3 : 'walk', #other
    4 : 'walk',
    5 : 'car',
    6 : 'car_passenger',
    7 : 'car_passenger',
    8 : 'car',
    9 : 'taxi',
    10 : 'pt',
    11 : 'pt',
    12 : 'pt',
    13 : 'pt', #should not be in the final dataset
    14 : 'pt',
    15 : 'pt',
    16 : 'pt',
    17 : 'pt',
    18 : 'pt', #school bus
    19 : 'pt',
    20 : 'pt',    
    21 : 'pt',
    22 : 'pt',
    23 : 'pt',
    24 : 'pt',
    25 : 'pt',
    26 : 'pt',
    27 : 'pt',
    28 : 'pt',
    29 : 'pt',
    99 : 'pt', #refused
    }

    mode_priority = {
    0 : 'walk',
    1 : 'pt',
    2 : 'car'
    }

    
    
    df_activity = pd.read_csv("%s/CHTS/survey_activity.csv" % context.config("data_path"))
    df_place = pd.read_csv("%s/CHTS/survey_place.csv" % context.config("data_path"))
    df_household = pd.read_csv("%s/CHTS/survey_households.csv" % context.config("data_path"), dtype={'home_count_id': float})
    df_persons = pd.read_csv("%s/CHTS/survey_person.csv" % context.config("data_path"))
    df_household = df_household[["sampno", "home_county_id","home_tract_id", "vehicle_count", "bike_count", "income"]]
	
    df_act_first = df_activity.sort_values(by=['sampno', 'perno', 'tripno'], ascending=True, na_position='first').drop_duplicates(subset=['sampno', 'perno', 'tripno'], keep='first')
    df_act_last = df_activity.sort_values(by=['sampno', 'perno', 'tripno'], ascending=True, na_position='first').drop_duplicates(subset=['sampno', 'perno', 'tripno'], keep='last')
    df_act_fl = pd.concat([df_act_first, df_act_last])
    df_act_fl = df_act_fl.sort_values(by=['sampno', 'perno', 'tripno'], ascending=True, na_position='first')
    #we need to extract arrival time from the first entry and departure time from the second entry to form activity
    #times at a single place
    df_act_first = df_act_first[['sampno', 'perno', 'plano', 'tripno', 'actno', 'arr_time','dep_time','purpose', 'travel_date']]
    df_act_last = df_act_last[['sampno', 'perno', 'plano', 'tripno', 'actno', 'arr_time','dep_time','purpose', 'travel_date']]
    df_act_fl = df_act_first.merge(df_act_last,on=['sampno', 'perno', 'plano', 'tripno'])
    df_act_fl["arr_time"]=df_act_fl['arr_time_x']
    df_act_fl["dep_time"]=df_act_fl['dep_time_y']
    df_act_fl = df_act_fl.rename(columns={'purpose_x' : 'purpose'})
    df_act_fl = df_act_fl.rename(columns={'travel_date_x' : 'travel_date'})
    df_act_fl = df_act_fl[['sampno', 'perno', 'plano', 'tripno', 'arr_time','dep_time','purpose', 'travel_date']]

    df_act_pl = df_act_fl.merge(df_place, on=['sampno', 'perno', 'plano', 'tripno'])
    df_act_pl = df_act_pl.rename(columns={'arr_time_x' : 'arr_time', 'dep_time_x' : 'dep_time', 'travel_date_x' : 'travel_date'})
    df_act_pl = df_act_pl[['sampno', 'perno', 'plano', 'tripno', 'arr_time','dep_time', 'travel_date', 'purpose', 'mode', 'tract_id', 'county_id', 'state_id', 'air_trip_distance_miles', 'trip_distance_miles']]

    df_act_pl = df_act_pl.merge(df_household[['sampno','home_county_id']], on=['sampno'])

    
    ## first we create a unique person_id
    df_act_pl["person_id"] = df_act_pl['sampno']*100 + df_act_pl['perno']
    print(len(df_act_pl["person_id"].unique()))
    ## we need to extract those that do not move, no matter if it is at home or not, 
    ## so they are kept later
    df_act_pl['travel_date'] = df_act_pl['travel_date'].astype('datetime64[ns]') 
    df_act_pl['DayName'] = df_act_pl["travel_date"].dt.day_name()
    df_act_pl = df_act_pl[df_act_pl['DayName'].isin(daysToKeep)]
    print(len(df_act_pl["person_id"].unique()))       
    df_act_pl = df_act_pl[df_act_pl['county_id'].isin(counties)]
    print(len(df_act_pl["person_id"].unique())) 
    df_act_pl = df_act_pl[df_act_pl['home_county_id'].isin(counties)]
    print(len(df_act_pl["person_id"].unique()))
    
    
    
    staying_person_ids = set(np.unique(df_act_pl[(df_act_pl['arr_time'].astype(str)=='03:00:00') & (df_act_pl['dep_time'].astype(str)=='02:59:00')]['person_id']))
    print(len(staying_person_ids))
    ## we need to keep only those that travel on wanted days of the week
    
    workdays_person_ids = set(np.unique(df_act_pl[df_act_pl['DayName'].isin(daysToKeep)]['person_id']))    
    #exit()
    df_staying = df_act_pl[df_act_pl['person_id'].isin(staying_person_ids)]

    ## we have to remove all persons that have travelled with a plane
    ## we also have to remove all persons that come from other counties
    ## remove all persons that transfer, but do not have a mode

    ##first those that travel by plane
    person_ids = set(np.unique(df_act_pl[df_act_pl["mode"]==13.0]["person_id"]))
    ##then all those that do not travel within the region
    #person_ids = person_ids | set(np.unique(df_act_pl[~df_act_pl['county_id'].isin(counties)]['person_id']))
   
    ##then all those that transfer but do not have a mode

    person_ids = person_ids | set(np.unique(df_act_pl[(df_act_pl['purpose'] == 21) & (df_act_pl['mode'].isna())]['person_id']))
    to_keep_ids = set(np.unique(df_act_pl["person_id"])) - person_ids
    to_remove_ids = person_ids
    
    df_act_pl = df_act_pl[df_act_pl["person_id"].isin(to_keep_ids)]
    print(len(df_act_pl["person_id"].unique()))
    ##assign activity types based on purpose
    df_act_pl["activity_type"] = df_act_pl["purpose"].apply(lambda x: activity_type.get(x))
    df_act_pl['tract_id'] = 6*1000000000 + df_act_pl['county_id']*1000000+df_act_pl['tract_id']
    df_act_pl = df_act_pl.merge(df_persons[['sampno','perno','empl_tract_id']], on=['sampno', 'perno'])
    i = 0
    ##those that do not work at their main workplace are classified as business
    for row in df_act_pl[["activity_type"]].itertuples(index = False): 
    
        if (row[0]=='business'):
            if (df_act_pl.iloc[i, df_act_pl.columns.get_loc('empl_tract_id')] == df_act_pl.iloc[i, df_act_pl.columns.get_loc('tract_id')]):
                df_act_pl.iloc[i, df_act_pl.columns.get_loc('activity_type')] == 'work'
        i = i + 1
    ## what is left is to merge certain stages into trips
    ## currently we have transfer places as activity locations
    ## so we need to remove those and only keep the main mode pt/car
    #df_transfer = df_act_pl[df_act_pl["purpose"]==21]
    
    df_act_pl["priority"] = df_act_pl["mode"].apply(lambda x: priorities.get(x))
    df_act_pl['mode_cat'] = df_act_pl["mode"].apply(lambda x: mode_type.get(x))

    # assign priority for each multi-stage trip
    # and assign mode as well
    pers_id = list(set(df_act_pl["person_id"].values.tolist() ))
    to_remove = []
    purposes = []
    i = 0
    priority = 0
    trip_distance = 0.0
    air_distance = 0.0
    found = False
    df_act_pl["new_air_dist"] = df_act_pl['air_trip_distance_miles']
    for row in df_act_pl[["person_id", "purpose", "mode", "priority", "air_trip_distance_miles", "trip_distance_miles"]].itertuples(index = False): 
        if (row[1]==21):
            found = True
            priority = np.maximum(priority, row[3])
            trip_distance = trip_distance + row[5]
            air_distance = air_distance + row[4] 
        elif (found):
            found = False
            df_act_pl.iloc[i, df_act_pl.columns.get_loc('priority')] = np.maximum(priority, row[3])
        
            df_act_pl.iloc[i, df_act_pl.columns.get_loc('mode_cat')] = mode_priority.get(np.maximum(priority, row[3]))
            df_act_pl.iloc[i, df_act_pl.columns.get_loc('air_trip_distance_miles')] = df_act_pl.iloc[i, df_act_pl.columns.get_loc('air_trip_distance_miles')] + air_distance
            df_act_pl.iloc[i, df_act_pl.columns.get_loc('trip_distance_miles')] = df_act_pl.iloc[i, df_act_pl.columns.get_loc('trip_distance_miles')] + trip_distance
            trip_distance = 0.0
            air_distance = 0.0
            priority = 0
        i = i + 1         
    
    df_act_pl_merged = df_act_pl[df_act_pl["purpose"] != 21]        
    ## now we also have to fix the trip number
    i = 0
    person_id = 0
    trip_no = 0
    for row in df_act_pl_merged[["person_id"]].itertuples(index = False): 
    
        if ((row[0] != person_id)):
            person_id = row[0]
            df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('tripno')] = 0
            trip_no = 1
        else:
            df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('tripno')] = trip_no
            trip_no = trip_no + 1
            person_id = row[0]
        i = i + 1


    ## we need only to take into account work days
    df_act_pl_merged['travel_date'] = df_act_pl_merged['travel_date'].astype('datetime64[ns]') 
    df_act_pl_merged['DayName'] = df_act_pl_merged["travel_date"].dt.day_name()
    df_act_pl_merged = df_act_pl_merged[df_act_pl_merged['DayName'].isin(daysToKeep)]
    df_act_pl_merged = df_act_pl_merged[~((df_act_pl_merged['arr_time'].astype(str)=='03:00:00') & (df_act_pl_merged['dep_time'].astype(str)=='02:59:00'))]
    ## we need to take only those that start and at home
    df_act_type_first = df_act_pl_merged.sort_values(by=['sampno', 'perno', 'tripno'], 
                                                 ascending=True, na_position='first').drop_duplicates(subset=['sampno', 'perno'], keep='first')
                                                                                                                                             
    df_act_type_last = df_act_pl_merged.sort_values(by=['sampno', 'perno', 'tripno'], 
                                                ascending=True, na_position='first').drop_duplicates(subset=['sampno', 'perno'], keep='last')                                                                                                                              

    df_temp = df_act_type_first.merge(df_act_type_last,on=['sampno', 'perno'])
    person_ids = set(np.unique(df_temp[(df_temp['activity_type_x']=='home') & (df_temp['activity_type_y']=='home')]['person_id_x']))
    df_act_pl_merged = df_act_pl_merged.merge(df_persons[['sampno','perno','perwgt']], on=['sampno','perno'])
    kept_weight = df_act_pl_merged[df_act_pl_merged["person_id"].isin(person_ids)].drop_duplicates(subset= ['person_id'], keep='first')['perwgt'].sum()

    old_weight = df_act_pl_merged.drop_duplicates(subset=['person_id'], keep='first')['perwgt'].sum()

    reweight = old_weight/(kept_weight)

    df_act_pl_merged = df_act_pl_merged[df_act_pl_merged['person_id'].isin(person_ids)]
    
    ##TODO we need to adjust the weights here
    
    
    
    print(len(df_act_pl_merged["person_id"].unique()))
    ##we need to adjust times to seconds after midnight of the day 1

    df_act_pl_merged["dep_time"] = df_act_pl_merged["dep_time"].apply(lambda x: np.dot(np.array(x.split(':'), dtype = np.int), [3600, 60, 1]))
    df_act_pl_merged["arr_time"] = df_act_pl_merged["arr_time"].apply(lambda x: np.dot(np.array(x.split(':'), dtype = np.int), [3600, 60, 1]))
    i = 0
    person_id = 0
    arrival_time = 3 * 3600
    departure_time = 0
    for row in df_act_pl_merged[["person_id"]].itertuples(index = False): 
    
        if ((row[0] != person_id)):
            if (df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('dep_time')] < df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('arr_time')]):
                df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('dep_time')] = df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('dep_time')] + 24 * 3600
        
            departure_time = df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('dep_time')]
            person_id = row[0]
        else:
            if (df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('arr_time')] < departure_time):
                df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('arr_time')] = df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('arr_time')] + 24 * 3600
            if (df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('dep_time')] < df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('arr_time')]):
                df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('dep_time')] = df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('dep_time')] + 24 * 3600
            departure_time = df_act_pl_merged.iloc[i, df_act_pl_merged.columns.get_loc('dep_time')]
            
        i = i + 1

    df_trips = df_act_pl_merged.copy()
    #this is done now above df_trips['tract_id'] = 6*1000000000 + df_trips['county_id']*1000000+df_trips['tract_id']

    ## we need to get df_trips to the right format
    df_following = pd.DataFrame(df_trips[[
        "person_id", "tripno", "dep_time", "activity_type", 'tract_id'
    ]], copy = True)
    df_following.columns = ["person_id", "tripno", "following_trip_departure_time", "origin_purpose", 'origin_zone']
    df_following["tripno"] = df_following["tripno"] + 1

    df_trips_new = df_trips.merge(df_following, on=['person_id', 'tripno'])
    del df_trips_new['dep_time']

    df_trips_new['departure_time'] = df_trips_new['following_trip_departure_time']
    del df_trips_new['following_trip_departure_time']

    df_trips_new['arrival_time'] = df_trips_new['arr_time']
    del df_trips_new['arr_time']

    df_trips_new['destination_purpose'] = df_trips_new['activity_type']
    df_trips_new['destination_zone'] = df_trips_new['tract_id']

    df_trips_new['air_dist_km'] = df_trips_new['air_trip_distance_miles'] * 1.60934
    df_trips_new['trip_dist_km'] = df_trips_new['trip_distance_miles'] * 1.60934
    del df_trips_new['air_trip_distance_miles']
    del df_trips_new['trip_distance_miles']
    

    df_trips = df_trips_new

    ## now we merge moving with not moving people

    moving_person_ids = set(np.unique(df_trips['person_id']))
    print(len(moving_person_ids))
    print(len(staying_person_ids))
    keep_person_ids_final = moving_person_ids | staying_person_ids
    len(moving_person_ids & staying_person_ids)

    ##trips are done and now we need to take care of the persons dataframe
    df_persons_merged = df_persons.merge(df_household, on=['sampno'])    
    df_persons_merged['person_id'] = df_persons_merged['sampno']*100 + df_persons_merged['perno']
    # we remove those without a household    
    
    ## we need to keep only those living in the study area

    df_persons_merged = df_persons_merged[['person_id', 'sampno','perno','gender','empl_status','transit_pass','home_county_id','home_tract_id','vehicle_count','bike_count','income','perwgt','nrel_agebin','employment','student','school_grade','driver_license']]

    df_persons_merged = df_persons_merged[df_persons_merged['home_county_id'].isin(counties)]
    df_trips = df_trips[df_trips["person_id"].isin(set(np.unique(df_persons_merged["person_id"])))]

    df_persons = df_persons_merged[df_persons_merged['person_id'].isin(keep_person_ids_final)]	
    print(df_persons['perwgt'].sum())
    df_persons.loc[df_persons["person_id"].isin(moving_person_ids), 'perwgt'] *= reweight
    print(df_persons['perwgt'].sum()) 
    #df_persons = pd.read_csv("%s/sf_bayarea_python.csv" % context.config("data_path"))
    df_ptaccessible_zones = context.stage("data.osm.add_pt_variable")[1]
    df_ptaccessible_zones["zone_id"] = df_ptaccessible_zones["zone_id"].astype(np.int64)
    old_len = len(df_persons)
    df_persons = pd.merge(df_persons, df_ptaccessible_zones, left_on=["home_tract_id"], right_on=["zone_id"], how="left")
    assert (old_len == len(df_persons))
   # for column in ["weight_trip", "number_of_bikes", "number_of_motorcycles", "weekday"]:
   #     del df_persons[column]

    print("Filling %d/%d observations with number_of_cars = 0" % (np.sum(df_persons["vehicle_count"].isna()), len(df_persons)))
    df_persons["vehicle_count"] = df_persons["vehicle_count"].fillna(0.0)
    df_persons["vehicle_count"] = df_persons["vehicle_count"].astype(np.int)
    df_persons["number_of_vehicles"] = df_persons["vehicle_count"]
    df_persons["number_of_bikes"] = df_persons["bike_count"]

   # print("Removing %d/%d observations with NaN personal_income" % (np.sum(df_persons["personal_income"].isna()), len(df_persons)))
   # df_persons = df_persons[~df_persons["personal_income"].isna()]

    #weight
    df_persons.loc[:, "person_id"] = df_persons["sampno"].astype(str) + "_" + df_persons["perno"].astype(str)
    df_persons.loc[:, "weight"] = df_persons["perwgt"]

    # Attributes
    df_persons["sex"] = "male"
    df_persons.loc[df_persons["gender"] == 1, "sex"] = "male"
    df_persons.loc[df_persons["gender"] == 2, "sex"] = "female"
    df_persons["sex"] = df_persons["sex"].astype("category")
     
    df_persons["has_license"] = df_persons["driver_license"] == 1.0
    df_persons["has_pt_subscription"] = df_persons["transit_pass"] == 1.0
    df_persons["__employment"] = df_persons["employment"]
    df_persons["employment"] = "no"
    df_persons.loc[df_persons["__employment"] == 1, "employment"] = "yes"
    df_persons.loc[df_persons["__employment"] == 2, "employment"] = "no"
    df_persons.loc[df_persons["student"] == 1, "employment"] = "student"
    df_persons.loc[df_persons["student"] == 2, "employment"] = "student"
    
    df_persons["employment"] = df_persons["employment"].astype("category")

    df_persons["age_class_hts"] = df_persons["nrel_agebin"].astype(np.int)
    if (len(context.config("counties")) < 3):
        df_persons.loc[df_persons["age_class_hts"]==1, "age_class_hts"] = 1
        df_persons.loc[df_persons["age_class_hts"]==2, "age_class_hts"] = 2
        df_persons.loc[df_persons["age_class_hts"]==3, "age_class_hts"] = 3
        df_persons.loc[df_persons["age_class_hts"]==4, "age_class_hts"] = 3
        df_persons.loc[df_persons["age_class_hts"]==5, "age_class_hts"] = 4
        df_persons.loc[df_persons["age_class_hts"]==6, "age_class_hts"] = 4
        df_persons.loc[df_persons["age_class_hts"]==7, "age_class_hts"] = 5
        df_persons.loc[df_persons["age_class_hts"]==8, "age_class_hts"] = 5

    
    df_persons["binary_car_availability"] = df_persons["vehicle_count"] > 0
    df_persons["binary_bike_availability"] = df_persons["bike_count"] > 0

    df_persons["sf_home"] = df_persons["home_county_id"]==75
    
    df_persons.loc[df_persons["sf_home"]== True, "sf_home"] = 1
    df_persons.loc[df_persons["sf_home"] == False, "sf_home"] = 0
    # Clean up
    df_persons = df_persons[[
        "person_id", "weight", "age_class_hts",
        "sex", "employment", "binary_car_availability", "binary_bike_availability",
        "number_of_vehicles", "number_of_bikes", "has_license", "has_pt_subscription", "sf_home", "pt_accessible", "income", "home_county_id"        
    ]]

    # Trips

    #df_trips = pd.read_csv("%s/sf_bayarea_trips_python.csv" % context.config("data_path"), sep = ",")
    #df_trips = df_trips.rename(columns = { "perno": "person_id" })
    df_trips = df_trips.rename(columns = { "tripno": "trip_id" })
    df_trips["person_id"] = df_trips["sampno"].astype(str) + "_" + df_trips["perno"].astype(str)

    df_trips.loc[df_trips["destination_purpose"] == "Shopping", "destination_purpose"] = "shop"
    df_trips.loc[df_trips["destination_purpose"] == "Home", "destination_purpose"] = "home"
    df_trips.loc[df_trips["destination_purpose"] == "Leisure", "destination_purpose"] = "leisure"
    df_trips.loc[df_trips["destination_purpose"] == "Work", "destination_purpose"] = "work"
    df_trips.loc[df_trips["destination_purpose"] == "Errands", "destination_purpose"] = "other"
    df_trips.loc[df_trips["destination_purpose"] == "Education", "destination_purpose"] = "education"
    df_trips.loc[df_trips["destination_purpose"] == "PD", "destination_purpose"] = "other"
    df_trips.loc[df_trips["destination_purpose"] == "Other", "destination_purpose"] = "other"

    df_trips["purpose"] = df_trips["destination_purpose"].astype("category")

    taxi_motor_person_ids = df_trips[df_trips["mode_cat"] == "taxi_motor"]["person_id"].drop_duplicates()
    df_persons["has_taxi_motor_trip"] = df_persons["person_id"].isin(taxi_motor_person_ids)
    df_persons.loc[df_persons["has_taxi_motor_trip"], "number_of_vehicles"] = df_persons["number_of_vehicles"] + 1

    df_trips["__mode"] = df_trips["mode_cat"]
    df_trips["mode"] = "car"

    df_trips.loc[df_trips["__mode"] == "car_passenger", "mode"] = "car_passenger"
    df_trips.loc[df_trips["__mode"] == "car", "mode"] = "car"
    df_trips.loc[df_trips["__mode"] == "walk", "mode"] = "walk"
    #normal
    df_trips.loc[df_trips["__mode"] == "bike", "mode"] = "bike"
    #San Diego
    if (len(context.config("counties")) < 3):
        df_trips.loc[df_trips["__mode"] == "bike", "mode"] = "walk"
    df_trips.loc[df_trips["__mode"] == "pt", "mode"] = "pt"
    df_trips.loc[df_trips["__mode"] == "taxi", "mode"] = "pt"

    df_trips["mode"] = df_trips["mode"].astype("category")

    #df_trips["departure_time"] = df_trips["departure_time"].apply(lambda x: np.dot(np.array(x.split(':'), dtype = np.int), [3600, 60, 1]))
    #df_trips["arrival_time"] = df_trips["arrival_time"].apply(lambda x: np.dot(np.array(x.split(':'), dtype = np.int), [3600, 60, 1]))

   # df_trips["departure_time"] = df_trips["departure_h"] * 3600.0 + df_trips["departure_m"] * 60.0
   # df_trips["arrival_time"] = df_trips["arrival_h"] * 3600.0 + df_trips["arrival_m"] * 60.0

    # Crowfly distance
    df_trips["air_dist_km"] = df_trips["air_dist_km"].astype(float)
    df_trips["crowfly_distance"] = df_trips["air_dist_km"] * 1000 * 3.28084

    # Adjust trip id
    #df_trips = df_trips.sort_values(by = ["perno", "tripno"])
    #trips_per_person = df_trips.groupby("perno").size().reset_index(name = "count")["count"].values
    #df_trips["new_trip_id"] = np.hstack([np.arange(n) for n in trips_per_person])


    # Impute activity duration
    #df_duration = pd.DataFrame(df_trips[[
    #    "person_id", "trip_id", "arrival_time"
    #]], copy = True)

    #df_following = pd.DataFrame(df_trips[[
    #    "person_id", "trip_id", "departure_time"
    #]], copy = True)
    #df_following.columns = ["person_id", "trip_id", "following_trip_departure_time"]
    #df_following["trip_id"] = df_following["trip_id"] - 1

    #df_duration = pd.merge(df_duration, df_following, on = ["person_id", "trip_id"])
    #df_duration["activity_duration"] = df_duration["following_trip_departure_time"] - df_duration["arrival_time"]
    #df_duration.loc[df_duration["activity_duration"] < 0.0, "activity_duration"] += 24.0 * 3600.0

    #df_duration = df_duration[["person_id", "trip_id", "activity_duration"]]
    #df_trips = pd.merge(df_trips, df_duration, how = "left", on = ["person_id", "trip_id"])

    df_shift = df_trips.shift(-1)
    df_trips["activity_duration"] = df_shift["departure_time"] - df_trips["arrival_time"]
    df_trips.loc[df_trips["person_id"] != df_shift["person_id"], "activity_duration"] = np.nan 
    df_trips.loc[df_trips["activity_duration"] < 0, "activity_duration"] += 24 * 3600 


    # Clean up
    df_trips = df_trips[[
        "person_id", "trip_id", "purpose", "mode",
        "departure_time", "arrival_time", "crowfly_distance", "activity_duration", "origin_purpose",
        "destination_purpose"
    ]]

    # Find everything that is consistent
    #existing_ids = set(np.unique(df_persons["person_id"])) & set(np.unique(df_trips["person_id"]))
    #df_persons = df_persons[df_persons["person_id"].isin(existing_ids)]
    #df_trips = df_trips[df_trips["person_id"].isin(existing_ids)]


    #### From here everything as Paris   
    
    # Contains car
    car_person_ids = df_trips[df_trips["mode"] == "car"]["person_id"].drop_duplicates()
    df_persons["has_car_trip"] = df_persons["person_id"].isin(car_person_ids)


    
    # Primary activity information
    df_education = df_trips[df_trips["purpose"] == "education"][["person_id"]].drop_duplicates()
    df_education["has_education_trip"] = True
    df_persons = pd.merge(df_persons, df_education, how = "left")
    df_persons["has_education_trip"] = df_persons["has_education_trip"].fillna(False)

    df_work = df_trips[df_trips["purpose"] == "work"][["person_id"]].drop_duplicates()
    df_work["has_work_trip"] = True
    df_persons = pd.merge(df_persons, df_work, how = "left")
    df_persons["has_work_trip"] = df_persons["has_work_trip"].fillna(False)

    # Find commute information
    df_commute = df_trips[df_trips["purpose"].isin(["work", "education"])]
    df_commute = df_commute.sort_values(by = ["person_id", "crowfly_distance"])
    df_commute = df_commute.drop_duplicates("person_id", keep = "last")

    df_commute = df_commute[["person_id", "crowfly_distance", "mode"]]
    df_commute.columns = ["person_id", "commute_distance", "commute_mode"]

    df_persons = pd.merge(df_persons, df_commute, on = "person_id", how = "left")

    assert(not df_persons[df_persons["has_work_trip"]]["commute_distance"].isna().any())
    assert(not df_persons[df_persons["has_education_trip"]]["commute_distance"].isna().any())

    # Passengers
    passenger_ids = df_trips[df_trips["mode"] == "car_passenger"]["person_id"].drop_duplicates()
    df_persons["is_passenger"] = df_persons["person_id"].isin(passenger_ids)
    
    # Filter people who are passengers at some point durign the day but also have a car trip
    
    #f = df_persons["is_passenger"] & df_persons["has_car_trip"]
    #print(np.sum(f) / len(df_persons))
    #exit()
    person_ids_trips = set(np.unique(df_trips["person_id"]))
    person_ids_persons = set(np.unique(df_persons["person_id"]))
    print(len(person_ids_trips))
    print(len(person_ids_persons))
    return df_persons, df_trips
