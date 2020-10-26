import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None # default='warn' ## turns of SettingWithCopyWarning
import geopandas as gpd
from shapely.geometry import Point
import shapely
from sklearn.utils import shuffle
import re
import censusdata as cd
import os

def configure(context):
	context.config("popgen_input_path")
	
# Function to insert row in the dataframe 
def Insert_row(row_number, df, row_value): 
    start_upper = 0
    end_upper = row_number 
    start_lower = row_number 
    end_lower = df.shape[0] 
    upper_half = [*range(start_upper, end_upper, 1)] 
    lower_half = [*range(start_lower, end_lower, 1)] 
    lower_half = [x.__add__(1) for x in lower_half] 
    index_ = upper_half + lower_half 
    df.index = index_ 
    df.loc[row_number] = row_value 
    df = df.sort_index() 
    return df  	
	
def execute(context):
    Counties = context.stage("county_names")
    #scale factor for the population
    scale = 1
    OutputFolder = context.config("popgen_input_path")
    
    Miscdict = {
    # population in group quaters and total population
    "grp_est":"B26001_001E",
    "TotPop":"S0101_C01_001E",
    }
    
    PersCtDict = {
        #"geo" : "GEO.id2",
        # Male / Female
        "pgender_1":"B01001_002E",
        "pgender_2":"B01001_026E",
    
        # Age distribution
        "page_1":"S0101_C01_002E",
        "page_2":"S0101_C01_003E",
        "page_3":"S0101_C01_004E",
        "page_4":"S0101_C01_005E",
        "page_5":"S0101_C01_006E",
        "page_6":"S0101_C01_007E",
        "page_7":"S0101_C01_008E",
        "page_8":"S0101_C01_009E",
        "page_9":"S0101_C01_010E",
        "page_10":"S0101_C01_011E",
        "page_11":"S0101_C01_012E",
        "page_12":"S0101_C01_013E",
        "page_13":"S0101_C01_014E",
        "page_14":"S0101_C01_015E",
        "page_15":"S0101_C01_016E",
        "page_16":"S0101_C01_017E",
        "page_17":"S0101_C01_018E",
        "page_18":"S0101_C01_019E",
    
        #employed
        "pemploy_1":"B23025_004E"    
    }
    invPersCtDict = {v: k for k, v in PersCtDict.items()}    
    
    TotPops = cd.download("acs5",2017,cd.censusgeo([('state', '06'),("county","*")]),[Miscdict["TotPop"]],tabletype="subject")
    grp_ests = cd.download("acs5",2017,cd.censusgeo([('state', '06'),("county","*")]),[Miscdict["grp_est"]])
    Totpops = TotPops.reset_index()
    grp_ests = grp_ests.reset_index()   
    
    scal_factors= dict()
    for County in Counties:
        grp_est = grp_ests[grp_ests["index"].astype("str").str.contains(County)][Miscdict["grp_est"]].sum()
        TotPop = Totpops[Totpops["index"].astype("str").str.contains(County)][Miscdict["TotPop"]].sum()
        scal_factors[County] = 1 - grp_est/TotPop
    
    # API cannot deal with multiple variables that are not of same tabletype.. Need to separate variables starting with B and S
    Blist=[]
    Slist=[]
    for key,val in PersCtDict.items():
        if val[0]=="B":
            Blist.append(val)
        if val[0]=="S":
            Slist.append(val)
    
    Ss = cd.download("acs5",2017,cd.censusgeo([('state', '06'),("county","*"),("tract","*")]),Slist,tabletype="subject")
    Bs = cd.download("acs5",2017,cd.censusgeo([('state', '06'),("county","*"),("tract","*")]),Blist)
    PersCT = pd.concat([Ss,Bs],axis=1,sort=True)
    PersCT = PersCT[PersCT.index.astype("str").str.contains(" County|".join(Counties)+ " County")]
    
    for County in Counties:
        PersCT[PersCT.index.astype("str").str.contains(County)] = PersCT[PersCT.index.astype("str").str.contains(County)].multiply(scal_factors[County]).round()
    
    PersCT.rename(columns=invPersCtDict,inplace=True)
    #we need to add number of unemployed as well
    PersCT['pemploy_2'] = PersCT['pgender_1'] + PersCT['pgender_2'] - PersCT['pemploy_1']
    PersCT = PersCT.div(scale).astype(int)
    
    PersCT.index = (PersCT.index.astype("str").str.extract("((?<=state:)[0-9]{2})")[0] +
                PersCT.index.astype("str").str.extract("((?<=county:)[0-9]{3})")[0] +
                PersCT.index.astype("str").str.extract("((?<=tract:)[0-9]{6})")[0]).to_list()
    PersCT = PersCT.reset_index()
    
    PersCT.rename(columns={'index':'geo'}, inplace=True)
    PersCT["geo"] = PersCT["geo"].str[1:]
    
    firstrow=[re.sub("_[0-9]{1,2}","",x) for x in PersCT.columns[1:]]
    firstrow=["variable_names"]+firstrow

    secrow=[x.split("_")[1] for x in PersCT.columns[1:]]
    secrow=["variable_categories"]+secrow

    thirdrow=["geo"]+(len(firstrow)-1)*[""]
    
    PersCT.columns=firstrow
    PersCT = Insert_row(0, PersCT, secrow)
    PersCT = Insert_row(1, PersCT, thirdrow)
    
    PersCT.to_csv(OutputFolder+"/person_marginals_age.csv",index=False)
    
    
    ## household level ##
    
    HouseCtDict = {
        #family/non-family
        "hhltype_1" : "B11016_002E",
        "hhltype_2" : "B11016_009E",
    
        #size Distribution
        "hhltypesize_1":"B11016_003E",
        "hhltypesize_2":"B11016_004E",
        "hhltypesize_3":"B11016_005E",
        "hhltypesize_4":"B11016_006E",
        "hhltypesize_5":"B11016_007E",
        "hhltypesize_6":"B11016_008E",
        "hhltypesize_7":"B11016_010E",
        "hhltypesize_8":"B11016_011E",
        "hhltypesize_9":"B11016_012E",
        "hhltypesize_10":"B11016_013E",
        "hhltypesize_11":"B11016_014E",
        "hhltypesize_12":"B11016_015E",
        "hhltypesize_13":"B11016_016E",
    
        #income distribution
        "hhlincome_1":"B19001_002E",
        "hhlincome_2":"B19001_003E",
        "hhlincome_3":"B19001_004E",
        "hhlincome_4":"B19001_005E",
        "hhlincome_5":"B19001_006E",
        "hhlincome_6":"B19001_007E",
        "hhlincome_7":"B19001_008E",
        "hhlincome_8":"B19001_009E",
        "hhlincome_9":"B19001_010E",
        "hhlincome_10":"B19001_011E",
        "hhlincome_11":"B19001_012E",
        "hhlincome_12":"B19001_013E",
        "hhlincome_13":"B19001_014E",
        "hhlincome_14":"B19001_015E",
        "hhlincome_15":"B19001_016E",
        "hhlincome_16":"B19001_017E",
    
        # vehicle distribution per household
        "hhlvehic_1":"DP04_0058E", # no vehicles
        "hhlvehic_2":"DP04_0059E", # 1 vehicle
        "hhlvehic_3":"DP04_0060E", # 2 vehicles
        "hhlvehic_4":"DP04_0061E", # 3 or more vehicles
    }
    invHouseCtDict = {v: k for k, v in HouseCtDict.items()}
    
    # API cannot deal with multiple variables that are not of same tabletype.. Need to separate variables starting with B and S
    # and D
    Blist=[]
    Dlist=[]

    for key,val in HouseCtDict.items():
        if val[0]=="B":
            Blist.append(val)
        if val[0]=="D":
            Dlist.append(val)
    
    Bs = cd.download("acs5",2017,cd.censusgeo([('state', '06'),("county","*"),("tract","*")]),Blist)
    Ds = cd.download("acs5",2017,cd.censusgeo([('state', '06'),("county","*"),("tract","*")]),Dlist,tabletype="profile")
    HouseCT= pd.concat([Bs,Ds],axis=1,sort=True)
    HouseCT = HouseCT[HouseCT.index.astype("str").str.contains(" County|".join(Counties)+ " County")]
    HouseCT.rename(columns=invHouseCtDict,inplace=True)
    
    HouseCT = HouseCT.div(scale).astype(int)
    HouseCT.index = (HouseCT.index.astype("str").str.extract("((?<=state:)[0-9]{2})")[0] +
                HouseCT.index.astype("str").str.extract("((?<=county:)[0-9]{3})")[0] +
                HouseCT.index.astype("str").str.extract("((?<=tract:)[0-9]{6})")[0]).to_list()
    HouseCT = HouseCT.reset_index()
    HouseCT.rename(columns={'index':'geo'}, inplace=True)
    HouseCT["geo"] = HouseCT["geo"].str[1:]
    
    firstrow=[re.sub("_[0-9]{1,2}","",x) for x in HouseCT.columns[1:]]
    firstrow=["variable_names"]+firstrow

    secrow=[x.split("_")[1] for x in HouseCT.columns[1:]]
    secrow=["variable_categories"]+secrow

    thirdrow=["geo"]+(len(firstrow)-1)*[""]
    
    HouseCT.columns=firstrow
    HouseCT = Insert_row(0, HouseCT, secrow)
    HouseCT = Insert_row(1, HouseCT, thirdrow)
    
    HouseCT.to_csv(OutputFolder+"/household_marginals_complete.csv",index=False)
    
    ## REGIONAL MARGINALS ##
    
    ## hosuehold level ##
    HousePumaDict = {
        # vehicle distribution per household
        "rhhlvehic_1":"DP04_0058E", # no vehicles
        "rhhlvehic_2":"DP04_0059E", # 1 vehicle
        "rhhlvehic_3":"DP04_0060E", # 2 vehicles
        "rhhlvehic_4":"DP04_0061E", # 3 or more vehicles
    }
    invHousePumaDict = {v: k for k, v in HousePumaDict.items()}
    HousePuma = cd.download("acs5",2017,cd.censusgeo([('state', '06'),("public use microdata area","*")]),list(HousePumaDict.values()),tabletype="profile")
    HousePuma = HousePuma[HousePuma.index.astype("str").str.contains(" County|".join(Counties)+ " County")]
    HousePuma.rename(columns=invHousePumaDict,inplace=True)
    HousePuma = HousePuma.div(scale).astype(int)
    HousePuma.index = HousePuma.index.astype("str").str.extract("((?<=public use microdata area:)[0-9]{5})")[0].to_list()
    HousePuma = HousePuma.reset_index()
    HousePuma.rename(columns={'index':'region'}, inplace=True)
    HousePuma["region"] = HousePuma["region"].astype("int")
    firstrow=[re.sub("_[0-9]{1,2}","",x) for x in HousePuma.columns[1:]]
    firstrow=["variable_names"]+firstrow

    secrow=[x.split("_")[1] for x in HousePuma.columns[1:]]
    secrow=["variable_categories"]+secrow

    thirdrow=["sample_geo"]+(len(firstrow)-1)*[""]
    HousePuma.columns=firstrow
    HousePuma = Insert_row(0, HousePuma, secrow)
    HousePuma = Insert_row(1, HousePuma, thirdrow)
    HousePuma.to_csv(OutputFolder+"/household_region_marginals.csv",index=False)


    ## person level ##
    
    PersPumaDict = {
        # Male / Female
        "rpgender_1":"B01001_002E",
        "rpgender_2":"B01001_026E",
    
        #employed
        "rpemploy_1":"B23025_004E",
    }
    invPersPumaDict = {v: k for k, v in PersPumaDict.items()}
    
    PersPuma = cd.download("acs5",2017,cd.censusgeo([('state', '06'),("public use microdata area","*")]),list(PersPumaDict.values()))
    # take only PUMS that we are interested in
    PersPuma = PersPuma[PersPuma.index.astype("str").str.contains(" County|".join(Counties)+ " County")]
    PersPuma = PersPuma.div(scale).astype(int)
    
    for County in Counties:
        PersPuma[PersPuma.index.astype("str").str.contains(County)] = PersPuma[PersPuma.index.astype("str").str.contains(County)].multiply(scal_factors[County]).round()
    PersPuma.rename(columns=invPersPumaDict,inplace=True)
    
    PersPuma.index = PersPuma.index.astype("str").str.extract("((?<=public use microdata area:)[0-9]{5})")[0].to_list()
    PersPuma = PersPuma.reset_index()
    PumaNums = PersPuma["index"].astype("int")
    PumaNums.to_csv(OutputFolder+"/regions.csv",index=False,header=False)
    
    PersPuma = PersPuma.astype("int")
    
    #we need to add number of unemployed as well
    PersPuma['rpemploy_2'] = PersPuma['rpgender_1'] + PersPuma['rpgender_2'] - PersPuma['rpemploy_1']
    
    firstrow=[re.sub("_[0-9]{1,2}","",x) for x in PersPuma.columns[1:]]
    firstrow=["variable_names"]+firstrow

    secrow=[x.split("_")[1] for x in PersPuma.columns[1:]]
    secrow=["variable_categories"]+secrow

    thirdrow=["sample_geo"]+(len(firstrow)-1)*[""]

    PersPuma.columns=firstrow
    PersPuma = Insert_row(0, PersPuma, secrow)
    PersPuma = Insert_row(1, PersPuma, thirdrow)
    PersPuma.to_csv(OutputFolder+"/person_marginals_complete.csv",index=False)
    
    ## PUMS Files ##
    data_pums_persons = pd.read_csv(OutputFolder + "/psam_p06.csv")
    data_pums_households = pd.read_csv(OutputFolder + "/psam_h06.csv")
    #keep only useful columns from persons
    useful_columns = ["AGEP","SERIALNO","PUMA","PWGTP","SEX","SPORDER", "ESR"]
    pums_persons = data_pums_persons.loc[:,useful_columns]
    #keep only useful columns from households
    useful_columns = ["SERIALNO","PUMA","WGTP","NP","VEH","HINCP","HHT"]
    pums_households =  data_pums_households.loc[:,useful_columns]

    pums_persons = pd.merge(pums_persons,pums_households,on='SERIALNO',how='left')

    ## Persons ##
    #separate the people that have a household weight > 0
    pums_persons = pums_persons[pums_persons['WGTP'] > 0]
    
    useful_columns = ["SERIALNO","PUMA_x","SPORDER", "AGEP", "SEX", "ESR"]
    pums_persons = pums_persons.loc[:,useful_columns]
    # only use data from pumas that are in PumaNums

    pums_persons_sample = pums_persons[pums_persons["PUMA_x"].isin(PumaNums)]

    pums_persons_sample['sample_geo']= pums_persons_sample['PUMA_x'] 
    pums_persons_sample['rpgender'] = pums_persons_sample['SEX']
    pums_persons_sample['pgender'] = pums_persons_sample['SEX']
    pums_persons_sample['pid']= pums_persons_sample['SPORDER']
    pums_persons_sample['hid']= pums_persons_sample['SERIALNO']
    pums_persons_sample['pemploy'] = 2
    pums_persons_sample['rpemploy'] = 2

    pums_persons_sample.loc[pums_persons_sample["ESR"] == 1, "pemploy"] = 1
    pums_persons_sample.loc[pums_persons_sample["ESR"] == 2, "pemploy"] = 1
    pums_persons_sample.loc[pums_persons_sample["ESR"] == 4, "pemploy"] = 1
    pums_persons_sample.loc[pums_persons_sample["ESR"] == 5, "pemploy"] = 1
    pums_persons_sample.loc[pums_persons_sample["ESR"] == 3, "pemploy"] = 2
    pums_persons_sample.loc[pums_persons_sample["ESR"] == 6, "pemploy"] = 2

    pums_persons_sample.loc[pums_persons_sample["ESR"] == 1, "rpemploy"] = 1
    pums_persons_sample.loc[pums_persons_sample["ESR"] == 2, "rpemploy"] = 1
    pums_persons_sample.loc[pums_persons_sample["ESR"] == 4, "rpemploy"] = 1
    pums_persons_sample.loc[pums_persons_sample["ESR"] == 5, "rpemploy"] = 1
    pums_persons_sample.loc[pums_persons_sample["ESR"] == 3, "rpemploy"] = 2
    pums_persons_sample.loc[pums_persons_sample["ESR"] == 6, "rpemploy"] = 2

    conditions_age = [
        (pums_persons_sample['AGEP'] < 5) ,
        (pums_persons_sample['AGEP'] >= 5) & (pums_persons_sample['AGEP'] <= 9),
        (pums_persons_sample['AGEP'] >= 10) & (pums_persons_sample['AGEP'] <= 14),
        (pums_persons_sample['AGEP'] >= 15) & (pums_persons_sample['AGEP'] <= 19),
        (pums_persons_sample['AGEP'] >= 20) & (pums_persons_sample['AGEP'] <= 24),
        (pums_persons_sample['AGEP'] >= 25) & (pums_persons_sample['AGEP'] <= 29),
        (pums_persons_sample['AGEP'] >= 30) & (pums_persons_sample['AGEP'] <= 34),
        (pums_persons_sample['AGEP'] >= 35) & (pums_persons_sample['AGEP'] <= 39),
        (pums_persons_sample['AGEP'] >= 40) & (pums_persons_sample['AGEP'] <= 44),
        (pums_persons_sample['AGEP'] >= 45) & (pums_persons_sample['AGEP'] <= 49),
        (pums_persons_sample['AGEP'] >= 50) & (pums_persons_sample['AGEP'] <= 54),
        (pums_persons_sample['AGEP'] >= 55) & (pums_persons_sample['AGEP'] <= 59),
        (pums_persons_sample['AGEP'] >= 60) & (pums_persons_sample['AGEP'] <= 64),
        (pums_persons_sample['AGEP'] >= 65) & (pums_persons_sample['AGEP'] <= 69),
        (pums_persons_sample['AGEP'] >= 70) & (pums_persons_sample['AGEP'] <= 74),
        (pums_persons_sample['AGEP'] >= 75) & (pums_persons_sample['AGEP'] <= 79),
        (pums_persons_sample['AGEP'] >= 80) & (pums_persons_sample['AGEP'] <= 84),
        (pums_persons_sample['AGEP'] >= 85)    
    ]
    choices_age = ['1', '2', '3','4', '5', '6','7', '8', '9','10', '11', '12', '13', '14', '15', '16', '17', '18']
    pums_persons_sample['page'] = np.select(conditions_age, choices_age, default='1')

    #re-arange the columns in the right order
    useful_columns = ["hid","pid","sample_geo","rpgender","pgender", "page", "rpemploy", "pemploy"]
    pums_persons_sample = pums_persons_sample.loc[:,useful_columns]
    
    #save the person sample file
    pums_persons_sample.to_csv(OutputFolder+"/person_sample_age.csv", index=False)

    ## Households ##
    pums_households_sample = pums_households[pums_households["PUMA"].isin(PumaNums)]
    pums_households_sample = pums_households_sample[pums_households_sample['WGTP'] > 0]
    pums_households_sample = pums_households_sample[pums_households_sample['NP'] > 0]

    pums_households_sample.loc[pums_households_sample['HHT'] == 1, 'HHT'] = '1'
    pums_households_sample.loc[pums_households_sample['HHT'] == 2, 'HHT'] = '1'
    pums_households_sample.loc[pums_households_sample['HHT'] == 3, 'HHT'] = '1'
    pums_households_sample.loc[pums_households_sample['HHT'] == 4, 'HHT'] = '2'
    pums_households_sample.loc[pums_households_sample['HHT'] == 5, 'HHT'] = '2'
    pums_households_sample.loc[pums_households_sample['HHT'] == 6, 'HHT'] = '2'
    pums_households_sample.loc[pums_households_sample['HHT'] == 7, 'HHT'] = '2'
    #pums_households_sample['rhhltype'] = pums_households_sample['HHT']
    pums_households_sample['hhltype'] = pums_households_sample['HHT']

    pums_households_sample['hid']=pums_households_sample['SERIALNO']
    pums_households_sample['sample_geo']=pums_households_sample['PUMA']

    conditions_hhltypesize = [
        (pums_households_sample['HHT'] == '1') & (pums_households_sample['NP'] == 2),
        (pums_households_sample['HHT'] == '1') & (pums_households_sample['NP'] == 3),
        (pums_households_sample['HHT'] == '1') & (pums_households_sample['NP'] == 4),
        (pums_households_sample['HHT'] == '1') & (pums_households_sample['NP'] == 5),
        (pums_households_sample['HHT'] == '1') & (pums_households_sample['NP'] == 6),
        (pums_households_sample['HHT'] == '1') & (pums_households_sample['NP'] >= 7),
        (pums_households_sample['HHT'] == '2') & (pums_households_sample['NP'] == 1),
        (pums_households_sample['HHT'] == '2') & (pums_households_sample['NP'] == 2),
        (pums_households_sample['HHT'] == '2') & (pums_households_sample['NP'] == 3),
        (pums_households_sample['HHT'] == '2') & (pums_households_sample['NP'] == 4),
        (pums_households_sample['HHT'] == '2') & (pums_households_sample['NP'] == 5),
        (pums_households_sample['HHT'] == '2') & (pums_households_sample['NP'] == 6),
        (pums_households_sample['HHT'] == '2') & (pums_households_sample['NP'] >= 7)
    ]
    choices_hhltypesize = ['1', '2', '3','4', '5', '6','7', '8', '9','10', '11', '12', '13']
    pums_households_sample['hhltypesize'] = np.select(conditions_hhltypesize, choices_hhltypesize, default='1')

    conditions_income = [
        (pums_households_sample['HINCP'] < 10000) ,
        (pums_households_sample['HINCP'] >= 10000) & (pums_households_sample['HINCP'] <= 14999),
        (pums_households_sample['HINCP'] >= 15000) & (pums_households_sample['HINCP'] <= 19999),
        (pums_households_sample['HINCP'] >= 20000) & (pums_households_sample['HINCP'] <= 24999),
        (pums_households_sample['HINCP'] >= 25000) & (pums_households_sample['HINCP'] <= 29999),
        (pums_households_sample['HINCP'] >= 30000) & (pums_households_sample['HINCP'] <= 34999),
        (pums_households_sample['HINCP'] >= 35000) & (pums_households_sample['HINCP'] <= 39999),
        (pums_households_sample['HINCP'] >= 40000) & (pums_households_sample['HINCP'] <= 44999),
        (pums_households_sample['HINCP'] >= 45000) & (pums_households_sample['HINCP'] <= 49999),
        (pums_households_sample['HINCP'] >= 50000) & (pums_households_sample['HINCP'] <= 59999),
        (pums_households_sample['HINCP'] >= 60000) & (pums_households_sample['HINCP'] <= 74999),
        (pums_households_sample['HINCP'] >= 75000) & (pums_households_sample['HINCP'] <= 99999),
        (pums_households_sample['HINCP'] >= 100000) & (pums_households_sample['HINCP'] <= 124999),
        (pums_households_sample['HINCP'] >= 125000) & (pums_households_sample['HINCP'] <= 149999),
        (pums_households_sample['HINCP'] >= 150000) & (pums_households_sample['HINCP'] <= 199999),
        (pums_households_sample['HINCP'] >= 200000)
    ]
    choices_income = ['1', '2', '3','4', '5', '6','7', '8', '9','10', '11', '12', '13','14','15','16']
    pums_households_sample['hhlincome'] = np.select(conditions_income, choices_income, default='1')

    conditions_vehicles = [
        (pums_households_sample['VEH'] == 0.0) ,
        (pums_households_sample['VEH'] == 1.0),
        (pums_households_sample['VEH'] == 2.0),
        (pums_households_sample['VEH'] >= 3.0)
    ]
    choices_vehicles = ['1', '2', '3','4']
    pums_households_sample['hhlvehic'] = np.select(conditions_vehicles, choices_vehicles, default='1')
    pums_households_sample['rhhlvehic'] = np.select(conditions_vehicles, choices_vehicles, default='1')


    #re-arange the columns in teh right order
    useful_columns = ["hid","sample_geo","hhltype", "hhlincome", "hhltypesize", "hhlvehic"]
    pums_households_sample = pums_households_sample.loc[:,useful_columns]
    pums_households_sample.to_csv(OutputFolder+"/household_sample_income.csv",index=False)
    
    ## relationship files ##
    ### read-in census to puma relationship file
    census_puma_relationship = pd.read_csv(OutputFolder + "/2010_Census_Tract_to_2010_PUMA.txt",dtype=str)
    census_puma_relationship = census_puma_relationship[census_puma_relationship['STATEFP']=='06']

    col="PUMA5CE"
    census_puma_relationship_sample = census_puma_relationship[census_puma_relationship[col].astype(int).isin(PumaNums)]
    census_puma_relationship_sample["geo"]=census_puma_relationship_sample["STATEFP"].astype(str) + census_puma_relationship_sample["COUNTYFP"].astype(str) + census_puma_relationship_sample["TRACTCE"].astype(str)
    useful_columns = ["PUMA5CE","geo"]

    census_puma_relationship_sample = census_puma_relationship_sample.loc[:,useful_columns]
       
    census_puma_relationship_sample.columns = ['region','geo']

    census_puma_relationship_sample['geo'] = census_puma_relationship_sample['geo'].str[1:]
    census_puma_relationship_sample['region'] = census_puma_relationship_sample['region'].astype(int)
    census_puma_relationship_sample.to_csv(OutputFolder+"/region_geo_mapping.csv",index=False)
    #write region_sample_mapping
    census_puma_relationship_sample['sample_geo'] = -1
    useful_columns = ["region","sample_geo"]
    census_puma_relationship_sample = census_puma_relationship_sample.loc[:,useful_columns]
    census_puma_relationship_sample["region"] = census_puma_relationship_sample["region"].astype(int)
    census_puma_relationship_sample.drop_duplicates().to_csv(OutputFolder+"/region_sample_mapping.csv",index=False)
    col="PUMA5CE"
    census_puma_relationship_sample = census_puma_relationship[census_puma_relationship[col].astype(int).isin(PumaNums)]
    
    census_puma_relationship_sample["geo"]=census_puma_relationship_sample["STATEFP"].astype(str).str[1:] + census_puma_relationship_sample["COUNTYFP"].astype(str) + census_puma_relationship_sample["TRACTCE"].astype(str)
    useful_columns = ["geo","PUMA5CE"]


    puma_censis_relationship_census = census_puma_relationship_sample.loc[:,useful_columns]
    puma_censis_relationship_census.columns = ['geo','sample_geo']
    puma_censis_relationship_census['sample_geo'] = puma_censis_relationship_census['sample_geo'].astype(int)

    puma_censis_relationship_census.to_csv(OutputFolder+"/geo_sample_mapping.csv",index=False)