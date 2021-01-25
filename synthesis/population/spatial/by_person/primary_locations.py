import gzip
from tqdm import tqdm
import pandas as pd
import numpy as np
import shapely.geometry as geo
import multiprocessing as mp

def configure(context):
    context.stage("synthesis.population.spatial.by_person.primary_zones")
    context.stage("data.spatial.zones")
    context.stage("synthesis.destinations")
    context.stage("synthesis.population.sociodemographics")
    context.stage("synthesis.population.sampled")
    context.config("processes")

SAMPLE_SIZE = 1000

def initialize_parallel(_df_persons, _df_locations):
    global df_persons, df_locations
    df_persons = pd.DataFrame(_df_persons, copy = True)
    df_locations = pd.DataFrame(_df_locations, copy = True) if _df_locations is not None else None

def define_ordering(df_persons, commute_coordinates):
    if "home_x" in df_persons.columns:
        home_coordinates = df_persons[["home_x", "home_y"]].values
        commute_distances = df_persons["commute_distance"].values
        indices = heuristic_ordering(home_coordinates, commute_coordinates, commute_distances)
        assert((np.sort(np.unique(indices)) == np.arange(len(commute_distances))).all())
        return indices
    else:
        return np.arange(len(commute_coordinates)) # Random ordering

def heuristic_ordering(home_coordinates, commute_coordinates, commute_distances):
    indices = []
    commute_indices = np.arange(len(commute_coordinates))

    for home_coordinate, commute_distance in zip(home_coordinates, commute_distances):
        distances = np.sqrt(np.sum((commute_coordinates - home_coordinate)**2, axis = 1))
        costs = np.abs(distances - commute_distance)
        costs[indices] = np.inf
        indices.append(np.argmin(costs))

    return indices

def bipartite_ordering(home_coordinates, commute_coordinates, commute_distances):
    import munkres
    x = commute_coordinates[:,0][np.newaxis, :] - home_coordinates[:,0][:, np.newaxis]
    y = commute_coordinates[:,1][np.newaxis, :] - home_coordinates[:,1][:, np.newaxis]
    distances = np.sqrt(x**2 + y**2)
    costs = np.abs(distances - commute_distances[:, np.newaxis])
    return [index[1] for index in munkres.Munkres().compute(costs)]

def run_parallel(args):
    i, chunk = args
    person_dfs = []

    for zone_id, count, shape in tqdm(
        chunk, desc = "Sampling coordinates", position = i):

        if count > 0:
            points = []
            ids = []

            if df_locations is None:
                while len(points) < count:
                    minx, miny, maxx, maxy = shape.bounds
                    candidates = np.random.random(size = (SAMPLE_SIZE, 2))
                    candidates[:,0] = minx + candidates[:,0] * (maxx - minx)
                    candidates[:,1] = miny + candidates[:,1] * (maxy - miny)
                    candidates = [geo.Point(*point) for point in candidates]
                    candidates = [point for point in candidates if shape.contains(point)]
                    points += candidates
                    ids += [np.nan] * len(candidates)

                points, ids = points[:count], ids[:count]
                points = np.array([np.array([point.x, point.y]) for point in points])
                ids = np.array([np.nan] * len(points))
            else:
                if np.count_nonzero(df_locations["zone_id"] == zone_id) == 0:
                    raise RuntimeError("Requested destination for a zone without discrete destinations")

                df_zone_locations = df_locations[df_locations["zone_id"] == zone_id]
                selector = np.random.randint(len(df_zone_locations), size = count)

                points = df_zone_locations.iloc[selector][["x", "y"]].values
                ids = df_zone_locations.iloc[selector]["location_id"].values

            f = df_persons["zone_id"] == zone_id
            ordering = define_ordering(df_persons[f], points)
            points, ids = points[ordering], ids[ordering]
            df_persons.loc[f, "x"] = points[:,0]
            df_persons.loc[f, "y"] = points[:,1]
            df_persons.loc[f, "location_id"] = ids
            person_dfs.append(df_persons[f])

    print() # Clean tqdm progress
    return pd.concat(person_dfs) if len(person_dfs) > 0 else pd.DataFrame()

def impute_locations(df_persons, df_zones, df_locations, threads, identifier = "person_id"):
    df_counts = df_persons[["zone_id"]].groupby("zone_id").size().reset_index(name = "count")
    df_zones = pd.merge(df_zones, df_counts, on = "zone_id", how = "inner")

    with mp.Pool(processes = threads, initializer = initialize_parallel, initargs = (df_persons, df_locations)) as pool:
        chunks = np.array_split(df_zones[["zone_id", "count", "geometry"]].values, threads)
        df_locations = pd.concat(pool.map(run_parallel, enumerate(chunks)))
        df_locations = df_locations[[identifier, "x", "y", "zone_id", "location_id"]]
        return df_locations

def execute(context):
    threads = context.config("processes")
    df_zones = context.stage("data.spatial.zones")[["zone_id", "geometry"]]
    df_commune_zones = context.stage("data.spatial.zones")
    df_opportunities = context.stage("synthesis.destinations")[0]
    df_commute = context.stage("synthesis.population.sociodemographics")[["person_id", "commute_distance"]]
    
    print("Imputing home locations ...")
    df_households = context.stage("synthesis.population.spatial.by_person.primary_zones")[0].copy()
    df_hhl = context.stage("synthesis.population.sampled").drop_duplicates("household_id")[[
        "household_id", "zone_id"
    ]].copy()

    df_hhl.rename(columns={"household_id":"person_id"}, inplace = True)
    df_home_opportunities = df_opportunities[df_opportunities["offers_home"]]

    df_home = impute_locations(df_hhl, df_zones, df_home_opportunities, threads, "person_id")[["person_id", "x", "y", "location_id"]]
    df_home.rename(columns = {"person_id":"household_id"}, inplace = True)
    df_hhl = context.stage("synthesis.population.sampled")
    df_home = pd.merge(df_hhl, df_home, on = ["household_id"], how = "left")
    del df_home["zone_id"]
    df_home = pd.merge(df_home, df_households[["person_id", "household_id"]], on = ["person_id", "household_id"], how = 'left')
    print(len(df_home))

    print("Imputing work locations ...")
    df_households =  context.stage("synthesis.population.spatial.by_person.primary_zones")[0].copy()
    df_work_zones =  context.stage("synthesis.population.spatial.by_person.primary_zones")[1].copy()
    print(df_work_zones)
    df_hw =  pd.merge(df_work_zones.rename(columns = {"zone_id":"work_id"}) , df_households.rename(columns = {"zone_id":"home_id"}), on=["person_id"], how='left')
    
    df_work_zones = pd.merge(df_hw, df_commute)
    df_work_zones = pd.merge(df_work_zones, df_home.rename({"x" : "home_x", "y" : "home_y"}, axis = 1))
    df_work_zones.rename(columns={"work_id" : "zone_id"},inplace=True)
    df_work_locations = df_opportunities[df_opportunities["offers_work"]]
    print(df_work_zones["zone_id"])
    df_work = impute_locations(df_work_zones, df_zones, df_work_locations, 24)[["person_id", "x", "y", "location_id"]]
    
    
    
    
        
    #df_persons = context.stage("synthesis.population.spatial.by_person.primary_zones")[1]
    #df_persons = pd.merge(df_persons, df_commute)
    #df_persons = pd.merge(df_persons, df_home.rename({"x" : "home_x", "y" : "home_y"}, axis = 1))
    #df_work_opportunities = df_opportunities[df_opportunities["offers_work"]]
    #df_work = impute_locations(df_persons, df_commune_zones, df_work_opportunities, threads)[["person_id", "x", "y", "location_id"]]
    print(len(df_work))
    return df_home, df_work
