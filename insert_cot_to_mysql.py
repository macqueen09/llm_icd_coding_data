import pandas as pd
import json
import csv
from tqdm import tqdm

from util_db import DBApi

db = DBApi()


def new_update_input_and_output():
    '''
    当前插入数据的代码，最新

    # text是病历原文
    # cot是qwen给出的解释
    # output 是 label
    select count(1) from mimic4.llm_train_data_labels where cot is not null ;
    当前3000条

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
    当前插入数据的代码，最新

    # text是病历原文
    # cot是qwen给出的解释
    # output 是 label
    select count(1) from mimic4.llm_train_data_labels where cot is not null ;
    当前3000条

    :return:
    '''
    df = pd.read_csv("./doubao_output_cot.csv",nrows=None)
    df.columns = ['subject_id', 'hadm_id', 'label','text', 'cot','instruction']
    print(df)
    print(df.columns)
    namelist = ['subject_id', 'hadm_id', 'label', 'text', 'cot', 'instruction']
    #for i in namelist:
    #    print(i,"\n",df.loc[1,i],'\n\n\n\n\n\n\n')

    # INSTRUCTION有点问题
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

