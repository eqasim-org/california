from tqdm import tqdm
import pandas as pd
import numpy as np
import simpledbf
import itertools

def configure(context):
    context.stage("data.spatial.zones")
    context.config("data_path")
    context.config("counties")
    context.config("county_names")

def execute(context):
    CityDict = {
    "Name":"San Francisco",
    "States":["CA"],
    #"CA":["San Diego"]}
    "CA": context.config("county_names")}
    StateDict = {    
    'CA':'California'}
    
    # create list of all counties necessary for worker extraction
    allCounties = [CityDict[x] for x in CityDict["States"]]
    allCounties = [item for sublist in allCounties for item in sublist] # flatten list
    first=True
    group = [] # list that gets filled by loaded data to concatenate later
    for State in CityDict["States"]:
        df = pd.read_csv("%s/CTPP/CA_2012thru2016_B302201.csv" % context.config("data_path"))

        # remove unnecessary columns
        df = df.drop(["SOURCE"],axis=1)

        # Link Data File and Table Shell through the table ID and line number.
        shell = pd.read_csv("%s/CTPP/2012-2016 CTPP documentation/acs_ctpp_2012thru2016_table_shell.txt" % context.config("data_path"),sep="|")
        df = df.merge(shell)

        Residence_map = pd.read_csv("%s/CTPP/2012-2016 CTPP documentation/acs_ctpp_2012thru2016_res_geo.txt" % context.config("data_path"), sep="|",encoding='latin-1')

        # not match with GEOID..
        Residence_map["SUMLEVEL"].unique()
        CT_map = Residence_map[Residence_map["GEOID"].str.contains("C1100US[0-9]{11}")]
        CT_map["CTID"] = CT_map["GEOID"].str.extract("([0-9]{11})")
    
        # only get the flow from Census tracts to census tracts
        df_CT = df[df["GEOID"].str.contains("C5400US[0-9]{11}")]

        # crop IDs from GEOID to get residence and workplace
        # 7 = length Prefix
        # 11= length CT ID
        df_CT["Res_ID"] = df_CT["GEOID"].str.slice(7,7+11)
        df_CT["Wrk_ID"] = df_CT["GEOID"].str.slice(7+11,7+11+11)
        # some workIds are from states different from the county. All Res Ids are in the same state

        df_CT = df_CT.merge(CT_map[["CTID","NAME"]],left_on="Res_ID",right_on="CTID")
        df_CT.rename(columns={"NAME":"RESIDENCE"},inplace=True)

        df_CT = df_CT.merge(CT_map[["CTID","NAME"]],left_on="Wrk_ID",right_on="CTID")
        df_CT.rename(columns={"NAME":"WORKPLACE"},inplace=True)
        df_CT.drop(["CTID_x","CTID_y"],axis=1,inplace=True)

        # expand indent line to more human readable column notation...
        for i in range(4):
            df_CT["INDENT "+str(i)] = df_CT[df_CT["LINDENT"]==i]["LDESC"]

        # ...fill the main indents to their subjects...
        df_CT.loc[df_CT["INDENT 2"].notnull(),["INDENT 3"]] = df_CT["INDENT 2"]
        df_CT.loc[df_CT["INDENT 1"].notnull(),["INDENT 2","INDENT 3"]] = df_CT["INDENT 1"]
        df_CT.loc[df_CT["INDENT 0"].notnull(),["INDENT 1","INDENT 2","INDENT 3"]]= df_CT["INDENT 0"]
    #for i in range(4):
    #    df_CT["INDENT "+str(i)] = df_CT["INDENT "+str(i)].ffill()
    Tdict={
    "Total":[0,5]}
    Lines=[2,3,4,5,6,7]

    boolmask = df_CT["LINENO"].isin(Lines)
    test = df_CT[boolmask]
    expanded = test.loc[test.index.repeat(test["EST"])].reset_index(drop=True)
    expanded = expanded[["Res_ID","Wrk_ID","LDESC","RESIDENCE","WORKPLACE"]]
    group.append(expanded)
        
    # State is in column "2"
    expanded["ResState"] = expanded["RESIDENCE"].str.split(',', expand=True)[2].str.strip()
    expanded["WrkState"] = expanded["WORKPLACE"].str.split(',', expand=True)[2].str.strip()    
    # only keep counties that are in its corresponding State
    for State in CityDict["States"]:
        expanded[expanded["ResState"]==StateDict[State]] = expanded[(expanded["ResState"]==StateDict[State]) & (expanded["RESIDENCE"].str.contains("|".join(CityDict[State])))]
        expanded[expanded["WrkState"]==StateDict[State]] = expanded[(expanded["WrkState"]==StateDict[State]) & (expanded["WORKPLACE"].str.contains("|".join(CityDict[State])))]
    
    # and then filter out all workers that are in other states
    allStates = [StateDict[x] for x in CityDict["States"]]
    expanded = expanded[expanded["WrkState"].isin(allStates)]
    expanded = expanded.dropna()
    print(expanded)
    
    df_od = expanded.groupby(["Res_ID", "Wrk_ID", "LDESC"]).count().reset_index()
    df_od = df_od[["Res_ID", "Wrk_ID", "LDESC", "RESIDENCE"]]
    df_od["Res_ID"] = df_od["Res_ID"].astype(str).str[1:].astype(int)
    df_od["Wrk_ID"] = df_od["Wrk_ID"].astype(str).str[1:].astype(int)
    return df_od
