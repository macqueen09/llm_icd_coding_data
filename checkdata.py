import pandas as pd
import json
import csv
from tqdm import tqdm

from util_db import DBApi

def func():
    path = '/supercloud/llm-data/icd10paper/4_mimic_iv/csv_note/'
    filename = 'discharge.csv' # (331793, 8)

    nrows=None
    nrows=300
    df = pd.read_csv(path+filename,nrows=nrows)
    print(df)
    print(df.shape)

    hadmid = df["hadm_id"]
    print(hadmid)
    print(hadmid.shape)
    h = hadmid.to_list()
    print(len(h))
    print(len(set(h)))

    return set(h)


def get_id():
    path = '/supercloud/llm-data/icd10paper/4_mimic_iv/mimic-iv-3.1/csv_hosp/'
    filename = 'diagnoses_icd.csv' # (6364488, 5)
    #filename = 'd_icd_diagnoses.csv' # (112107, 3)

    nrows=None
    nrows=300
    df = pd.read_csv(path+filename,nrows=nrows)
    print(df)
    print(df.shape)
    a = df[df["icd_version"] == 10]
    #print(a)
    h_list = a["hadm_id"]
    print(len(set(h_list)))

    icd10id = set(h_list)
    return icd10id
    

#textid = func()        
#icd10id = get_id()
#
#res = textid & icd10id
#print("--")
#print(len(res))



def get_has_id():
    path = '/supercloud/llm-data/icd10paper/4_mimic_iv/mimic-iv-3.1/csv_hosp/'
    #filename = 'diagnoses_icd.csv' # (6364488, 5)
    filename = 'd_icd_diagnoses.csv' # (112107, 3)

    nrows=None
    nrows=100
    df = pd.read_csv(path+filename,nrows=nrows)
    print(df)
    print(df.shape)

get_has_id()
