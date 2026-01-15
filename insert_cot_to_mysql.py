import pandas as pd
import json
import csv
from tqdm import tqdm

from util_db import DBApi

db = DBApi()


def new_update_input_and_output():
    '''
    Latest code for inserting data

    # text is the original medical record
    # cot is the explanation provided by qwen
    # output is the label
    select count(1) from mimic4.llm_train_data_labels where cot is not null ;
    Currently 3000 entries

    :return:
'''

    df = pd.read_csv("/supercloud/Raysome_171_server/commonutils_moreacc_20250220/llm_mark/online/icd10/online/en_paper_pipeline/CoT_data/llm_data_train_cot_all_1.csv",nrows=None)
    print(df)

    return

    for i in range(df.shape[0]):
        subject_id,hadm_id,label,text,cot,instruction = df.loc[i,["subject_id","hadm_id","label","text","cot","instruction"]]
        subject_id = subject_id.replace("tensor","").replace("(","").replace(")","")
        hadm_id = hadm_id.replace("tensor","").replace("(","").replace(")","")


        sql = '''
            update mimic4.llm_train_data_labels set text = '{}', cot = '{}', output = '{}', instruction = '{}' where subject_id = {} and hadm_id = {};
        '''.format(text.replace("\'","\""), cot.replace("\'","\""), label.replace("\'","\""), instruction.replace("\'","\""), subject_id, hadm_id)
        # print(sql)

        status = db.insert(sql)
        print(i,status,subject_id,hadm_id)
        # break


#new_update_input_and_output()

def update_table_from_doubao():
    '''
    Latest code for inserting data

    # text is the original medical record
    # cot is the explanation provided by qwen
    # output is the label
    select count(1) from mimic4.llm_train_data_labels where cot is not null ;
    Currently 3000 entries

    :return:
    '''
    df = pd.read_csv("./doubao_output_cot.csv",nrows=None)
    df.columns = ['subject_id', 'hadm_id', 'label','text', 'cot','instruction']
    print(df)
    print(df.columns)
    namelist = ['subject_id', 'hadm_id', 'label', 'text', 'cot', 'instruction']
    #for i in namelist:
    #    print(i,"\n",df.loc[1,i],'\n\n\n\n\n\n\n')

    # There are some issues with the INSTRUCTION
    #print(df.loc[1,"cot"])


    for i in range(df.shape[0]):
        subject_id,hadm_id,label,text,cot,instruction = df.loc[i,["subject_id","hadm_id","label","text","cot","instruction"]]
        subject_id = subject_id.replace("tensor","").replace("(","").replace(")","")
        hadm_id = hadm_id.replace("tensor","").replace("(","").replace(")","")


        sql = '''
            update mimic4.llm_train_data_labels set text = '{}', cot = '{}', output = '{}', instruction = '{}' where subject_id = {} and hadm_id = {};
        '''.format(text.replace("\'","\""), cot.replace("\'","\""), label.replace("\'","\""), instruction.replace("\'","\""), subject_id, hadm_id)
        # print(sql)

        status = db.insert(sql)
        print(i,status,subject_id,hadm_id)
        # break

update_table_from_doubao()

db.close()

