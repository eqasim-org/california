import pandas as pd
import numpy as np
from tqdm import tqdm
from sklearn.neighbors import KDTree
import multiprocessing as mp
from functools import partial

def configure(context):
    context.stage("data.hts.cleaned")
    context.stage("synthesis.destinations")
    context.stage("synthesis.population.spatial.by_person.primary_locations")
    context.stage("synthesis.population.spatial.by_person.primary_zones")
    context.stage("synthesis.population.sociodemographics")
    context.stage("synthesis.population.trips")
    context.config("processes")
    
def impute_education_locations_same_zone( df_ag, hts_trips, df_candidates, df_travel, age_min, age_max, age_group, name):
    hts_educ = hts_trips.copy()
    hts_educ = hts_educ[hts_educ["age_class_hts"] == age_group]
    hts_educ_cp = hts_educ[hts_educ["mode"]=="car_passenger"]
    hts_educ_ncp = hts_educ[hts_educ["mode"]!="car_passenger"]

    dist_educ_cp = hts_educ_cp#["crowfly_distance"]
    hist_cp, bins_cp = np.histogram(dist_educ_cp["crowfly_distance"], weights = dist_educ_cp["weight"], bins = 500)

    dist_educ_ncp = hts_educ_ncp#["crowfly_distance"]
    hist_ncp, bins_ncp = np.histogram(dist_educ_ncp["crowfly_distance"], weights = dist_educ_ncp["weight"], bins = 500)
    
    df_trips = df_travel.copy()

    df_trips = df_trips[np.logical_and(df_trips["age"] >= age_min, df_trips["age"] < age_max)]

    cp_ids = list(set(df_trips[df_trips["mode"]=="car_passenger"]["hts_person_id"].values))

    df_agents = df_ag.copy()
    df_agents = df_agents[np.logical_and(df_agents["age"] >= age_min, df_agents["age"] < age_max)]
    df_agents_cp  = df_agents[np.isin(df_agents["hts_person_id"], cp_ids)]
    df_agents_ncp  = df_agents[np.logical_not(np.isin(df_agents["hts_person_id"], df_agents_cp["hts_person_id"]))]

    assert len(df_agents_cp) + len(df_agents_ncp) == len(df_agents)

    home_coordinates_cp = list(zip(df_agents_cp["home_x"], df_agents_cp["home_y"]))
    home_coordinates_ncp = list(zip(df_agents_ncp["home_x"], df_agents_ncp["home_y"]))
    education_coordinates = np.array(list(zip(df_candidates["x"], df_candidates["y"])))

    bin_midpoints = bins_cp[:-1] + np.diff(bins_cp)/2
    cdf = np.cumsum(hist_cp)
    cdf = cdf / cdf[-1]
    values = np.random.rand(len(df_agents_cp))
    value_bins = np.searchsorted(cdf, values)
    random_from_cdf_cp = bin_midpoints[value_bins] # in meters
    tree = KDTree(education_coordinates)
    indices_cp, distances_cp = tree.query_radius(home_coordinates_cp, r=random_from_cdf_cp, return_distance = True, sort_results=True)

    bin_midpoints = bins_ncp[:-1] + np.diff(bins_ncp)/2
    cdf = np.cumsum(hist_ncp)
    cdf = cdf / cdf[-1]
    values = np.random.rand(len(df_agents_ncp))
    value_bins = np.searchsorted(cdf, values)
    random_from_cdf_ncp = bin_midpoints[value_bins] # in meters
    
    #tree = KDTree(education_coordinates)
    indices_ncp, distances_ncp = tree.query_radius(home_coordinates_ncp, r=random_from_cdf_ncp, return_distance = True, sort_results=True)

    # In some cases no education facility was found within the given radius. In this case select the nearest facility.
    for i in range(len(indices_cp)):
        l = indices_cp[i] 
        if len(l) == 0:
            dist, ind = tree.query(np.array(home_coordinates_cp[i]).reshape(1,-1), 2, return_distance = True, sort_results=True)
            fac = ind[0][1]
            indices_cp[i] = [fac]
            distances_cp[i] = [dist[0][1]]

    indices_cp = [l[-1] for l in indices_cp]
    distances_cp = [d[-1] for d in distances_cp]

    for i in range(len(indices_ncp)):
        l = indices_ncp[i] 
        if len(l) == 0:
            dist, ind = tree.query(np.array(home_coordinates_ncp[i]).reshape(1,-1), 2, return_distance = True, sort_results=True)
            fac = ind[0][1]
            indices_ncp[i] = [fac]
            distances_ncp[i] = [dist[0][1]]

    indices_ncp = [l[-1] for l in indices_ncp]
    distances_ncp = [d[-1] for d in distances_ncp]

    #f_persons = (df_agents["zone_id_x"] == df_agents["zone_id_y"])
    df_return_cp = df_agents_cp.copy()
    df_return_cp["x"] = df_candidates.iloc[indices_cp]["x"].values
    df_return_cp["y"] = df_candidates.iloc[indices_cp]["y"].values
    df_return_cp["location_id"]  = df_candidates.iloc[indices_cp]["location_id"].values

    df_return_ncp = df_agents_ncp.copy()
    df_return_ncp["x"] = df_candidates.iloc[indices_ncp]["x"].values
    df_return_ncp["y"] = df_candidates.iloc[indices_ncp]["y"].values
    df_return_ncp["location_id"]  = df_candidates.iloc[indices_ncp]["location_id"].values

    df_return = pd.concat([df_return_cp, df_return_ncp])
    assert len(df_return) == len(df_agents)
    return df_return
def parallelize_dataframe(hts_trips, df_ag, df_candidates, df_travel, age_min, age_max, age_group, name, func, n_cores=1):
    df_split = np.array_split(df_ag, n_cores)
    pool = mp.Pool(n_cores)
    prod_x=partial(func, hts_trips=hts_trips,df_candidates=df_candidates, df_travel=df_travel, age_min=age_min, age_max=age_max, age_group=age_group,name=name)
    df_locations = pd.concat(pool.map(prod_x, df_split))
    pool.close()
    pool.join()
    return df_locations
     
def execute(context):
    threads = context.config("processes")

    df_persons = context.stage("synthesis.population.sociodemographics")
    df_households = context.stage("synthesis.population.spatial.by_person.primary_zones")[0]
    df_home = context.stage("synthesis.population.spatial.by_person.primary_locations")[0]
    df_persons = pd.merge(df_persons, df_home.rename({"x" : "home_x", "y" : "home_y"}, axis = 1))
    df_persons_same_zone = pd.merge(df_persons,df_households,on=["person_id"], how='left')
   
    #df_persons_same_zone = df_persons_same_zone[df_persons_same_zone["zone_id_x"]==df_persons_same_zone["zone_id_y"]]
    df_education_locations = context.stage("synthesis.destinations")[1]
    df_education_locations = df_education_locations[df_education_locations["offers_education"]]
    #f_persons = (df_persons_same_zone["zone_id_x"] == df_persons_same_zone["zone_id_y"])
        
    df_candidates = df_education_locations.copy()

    df_hts_trips = context.stage("data.hts.cleaned")[1]
    df_hts_persons = context.stage("data.hts.cleaned")[0]
    df_hts = pd.merge(df_hts_trips, df_hts_persons, on=["person_id"])
    hts_trips_educ = df_hts[df_hts["purpose"]=="education"]

    df_agents = df_persons_same_zone.copy()
    df_agents = df_agents[df_agents["has_education_trip"]==True]
    df_trips = context.stage("synthesis.population.trips")

    df_education_types = context.stage("synthesis.destinations")[1]
    education_types = ["school", "univeristy"]


    df_agents = df_agents[["age", "hts_person_id", "home_x", "home_y", "person_id"]]

    df_trips = df_trips[df_trips["destination_purpose"]=='education']
    
    df_candidates = df_education_locations.copy()
    df_candidates = df_candidates[(df_candidates["type"]=='ELEM') | (df_candidates["type"]=='PS') | (df_candidates["type"]=='ELEMHIGH')]
    #educ_0_10 = impute_education_locations_same_zone(df_agents, hts_trips_educ, df_candidates, df_trips, 0,  10, 1, "/nas/balacm/educ016.png")
    educ_0_10 = parallelize_dataframe(hts_trips_educ, df_agents, df_candidates, df_trips, 0,  10, 1, "/nas/balacm/educ016.png", impute_education_locations_same_zone, 24)

    df_candidates = df_education_locations.copy()
    df_candidates = df_candidates[(df_candidates["type"]=='ELEMHIGH') | (df_candidates["type"]=='HS') | (df_candidates["type"]=='INTMIDJR')]
    #educ_11_18 = impute_education_locations_same_zone(df_agents, hts_trips_educ, df_candidates, df_trips, 10,  18, 1, "/nas/balacm/educ016.png")
    educ_11_18 = parallelize_dataframe(hts_trips_educ, df_agents, df_candidates, df_trips, 10,  18, 1, "/nas/balacm/educ016.png", impute_education_locations_same_zone, 24)

    df_candidates = df_education_locations.copy()
    df_candidates = df_candidates[(df_candidates["type"]=='college')]
    #educ_19_100 = impute_education_locations_same_zone(df_agents, hts_trips_educ, df_candidates, df_trips, 18,  100, 2, "/nas/balacm/educ016.png")
    educ_19_100 = parallelize_dataframe(hts_trips_educ, df_agents, df_candidates, df_trips, 18,  100, 2, "/nas/balacm/educ16100.png",impute_education_locations_same_zone, 24)

    education_locations = pd.concat([educ_0_10, educ_11_18, educ_19_100])   
    df_education = education_locations[["person_id", "x", "y", "location_id"]]
    return df_education