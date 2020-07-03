import pandas as pd
import subprocess as sp
import os.path

def configure(context):
    context.config("data_path")

def execute(context):
    df_schools = pd.read_csv("%s/education/pubschls.txt" % context.config("data_path"), sep="\t")
    df_schools = df_schools[((df_schools['EILCode']=='HS') | (df_schools['EILCode']=='ELEM') | (df_schools['EILCode']=='ELEMHIGH') | 
    (df_schools['EILCode']=='INTMIDJR') | (df_schools['EILCode']=='PS'))]
    df_schools["x"] = df_schools["Longitude"]
    df_schools["y"] = df_schools["Latitude"]
    df_schools["purpose"] = "education"
    df_schools["type"] = df_schools["EILCode"]
    df_schools = df_schools[['x','y','type','purpose']]
    df_schools = df_schools[~(df_schools["x"]=='No Data')]
    df_col = pd.read_csv("%s/education/Colleges_and_Universities.csv" % context.config("data_path"), sep=",")
    df_col["x"] = df_col["LONGITUDE"]
    df_col["y"] = df_col["LATITUDE"]
    df_col["purpose"] = "education"
    df_col["type"] = "college"
    df_col = df_col[['x','y','type','purpose']]
    df_education = pd.concat([df_schools,df_col])
    return df_education
    