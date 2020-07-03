from tqdm import tqdm
import pandas as pd
import numpy as np

def configure(context):
	context.stage("data.osm.add_pt_variable")
	context.config("data_path")


def execute(context):
    df_persons = pd.read_csv("%s/sf_bayarea_python.csv" % context.config("data_path"))
    df_ptaccessible_zones = context.stage("data.osm.add_pt_variable")[1]
    df_ptaccessible_zones["zone_id"] = df_ptaccessible_zones["zone_id"].astype(np.int64)

    #TODO: check the merging information
    df_persons = pd.merge(df_persons,df_ptaccessible_zones, left_on=["home_tract_id"], right_on=["zone_id"], how="left")

   # for column in ["weight_trip", "number_of_bikes", "number_of_motorcycles", "weekday"]:
   #     del df_persons[column]

    print("Filling %d/%d observations with number_of_cars = 0" % (np.sum(df_persons["vehicle_count"].isna()), len(df_persons)))
    df_persons["vehicle_count"] = df_persons["vehicle_count"].fillna(0.0)
    df_persons["vehicle_count"] = df_persons["vehicle_count"].astype(np.int)
    df_persons["number_of_vehicles"] = df_persons["vehicle_count"]
    df_persons["number_of_bikes"] = df_persons["bike_count"]

   # print("Removing %d/%d observations with NaN personal_income" % (np.sum(df_persons["personal_income"].isna()), len(df_persons)))
   # df_persons = df_persons[~df_persons["personal_income"].isna()]

    # ID and weight
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
    df_persons["binary_car_availability"] = df_persons["vehicle_count"] > 0
    df_persons["binary_bike_availability"] = df_persons["bike_count"] > 0

    df_persons["sf_home"] = df_persons["home_county_id"]==75
    
    df_persons.loc[df_persons["sf_home"]== True, "sf_home"] = 1
    df_persons.loc[df_persons["sf_home"] == False, "sf_home"] = 0
    # Clean up
    df_persons = df_persons[[
        "person_id", "weight", "age_class_hts",
        "sex", "employment", "binary_car_availability", "binary_bike_availability",
        "number_of_vehicles", "number_of_bikes", "has_license", "has_pt_subscription", "sf_home", "pt_accessible"        
    ]]

    # Trips

    df_trips = pd.read_csv("%s/sf_bayarea_trips_python.csv" % context.config("data_path"), sep = ",")
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
    df_trips.loc[df_trips["__mode"] == "bike", "mode"] = "bike"
    df_trips.loc[df_trips["__mode"] == "pt", "mode"] = "pt"
    df_trips.loc[df_trips["__mode"] == "taxi", "mode"] = "pt"

    df_trips["mode"] = df_trips["mode"].astype("category")

    df_trips["departure_time"] = df_trips["departure_time"].apply(lambda x: np.dot(np.array(x.split(':'), dtype = np.int), [3600, 60, 1]))

    #print(df_trips["departure_time"])
    #exit()
    df_trips["arrival_time"] = df_trips["arrival_time"].apply(lambda x: np.dot(np.array(x.split(':'), dtype = np.int), [3600, 60, 1]))

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

    return df_persons, df_trips
