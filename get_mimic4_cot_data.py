import pandas as pd
import json
import csv
from tqdm import tqdm

from util_db import DBApi

db = DBApi()

def insert_to_cot_text():
    sql = '''
    select di.subject_id as sid,
        di.hadm_id as hid,
        di.seq_num as seqnum,
        di.icd_code as icdcode,  
        did.long_title as long_title,
        d.text as ttext
    from mimic4.discharge d 
    join (
        select distinct subject_id,hadm_id from mimic4.diagnoses_icd di 
        where icd_version = 10
        limit 500
    ) srcid on d.subject_id = srcid.subject_id and d.hadm_id = srcid.hadm_id 
    left join mimic4.diagnoses_icd di on d.subject_id = di.subject_id and d.hadm_id = di.hadm_id
    left join mimic4.d_icd_diagnoses did on di.icd_code = did.icd_code and di.icd_version = 10
    where did.icd_code is not null
    order by di.hadm_id, di.seq_num 
    ;
    '''


    data = db.query(sql)

    df = pd.DataFrame.from_records(data)
    df.columns = ['subject_id', 'hadm_id', 'seq_num', 'icd_code', 'long_title', 'text']

    alldata = {}
    for i in range(df.shape[0]):
    #     # print(i)
        subject_id, hadm_id, seq_num, icd_code, long_title, text = df.loc[i,['subject_id', 'hadm_id', 'seq_num', 'icd_code', 'long_title', 'text']]
        # print(subject_id, hadm_id, seq_num, icd_code, long_title)
        key = str(subject_id) + "_" + str(hadm_id)
        if key not in alldata.keys():
            alldata[key] = {
                'subject_id': subject_id,
                "hadm_id":hadm_id,
                "icd_code":[],
                "text":text
            }
        alldata[key]['icd_code'].append(icd_code)

        long_title = long_title.replace("\'","\"")

        # 单个项插入到cot_diag_mark
        inset_sql = '''
            insert into mimic4.cot_diag_mark (subject_id, hadm_id, seq_num, icd10_code, icd10_name, text_unique_id) VALUES ({},{},{},'{}','{}','{}')
        '''.format(subject_id, hadm_id, seq_num, icd_code, long_title, key)
        db.insert(inset_sql)

    # 整个内容插入到cot_text
    for (key,v) in alldata.items():
        subject_id = v['subject_id']
        hadm_id = v["hadm_id"]
        icd_code = ",".join(v["icd_code"])
        text = v["text"].replace("\'","\"")
        # text = ''
        # print(icd_code)
        print(len(text))

        # 再考虑下怎么处理数据
        insert_sql = '''
        insert into mimic4.cot_text (subject_id, hadm_id, text, src_icd_codes, text_unique_id) VALUES ({},{},'{}','{}','{}')
        '''.format(subject_id, hadm_id, text, icd_code, key)
        db.insert(insert_sql)

# insert_to_cot_text()


def get_model_train_data():
    sql = '''
    select di.subject_id as sid,
        di.hadm_id as hid,
        di.seq_num as seqnum,
        di.icd_code as icdcode,  
        did.long_title as long_title,
        d.text as ttext
    from mimic4.discharge d 
    join (
        select distinct subject_id,hadm_id from mimic4.diagnoses_icd di 
        where icd_version = 10
        limit 500
    ) srcid on d.subject_id = srcid.subject_id and d.hadm_id = srcid.hadm_id 
    left join mimic4.diagnoses_icd di on d.subject_id = di.subject_id and d.hadm_id = di.hadm_id
    left join mimic4.d_icd_diagnoses did on di.icd_code = did.icd_code and di.icd_version = 10
    where did.icd_code is not null
    order by di.hadm_id, di.seq_num 
    ;
    '''

    data = db.query(sql)

    df = pd.DataFrame.from_records(data)
    df.columns = ['subject_id', 'hadm_id', 'seq_num', 'icd_code', 'long_title', 'text']

    alldata = {}
    for i in range(df.shape[0]):
    #     # print(i)
        subject_id, hadm_id, seq_num, icd_code, long_title, text = df.loc[i,['subject_id', 'hadm_id', 'seq_num', 'icd_code', 'long_title', 'text']]
        # print(subject_id, hadm_id, seq_num, icd_code, long_title)
        key = str(subject_id) + "_" + str(hadm_id)
        if key not in alldata.keys():
            alldata[key] = {
                'subject_id': subject_id,
                "hadm_id":hadm_id,
                "icd_code":[],
                "text":text,
                "label_code_title":{}
            }
        alldata[key]['icd_code'].append(icd_code)
        alldata[key]['label_code_title'][icd_code]=long_title

        # long_title = long_title.replace("\'","\"")
        # # 单个项插入到cot_diag_mark
        # inset_sql = '''
        #     insert into mimic4.cot_diag_mark (subject_id, hadm_id, seq_num, icd10_code, icd10_name, text_unique_id) VALUES ({},{},{},'{}','{}','{}')
        # '''.format(subject_id, hadm_id, seq_num, icd_code, long_title, key)
        # db.insert(inset_sql)

    # 整个内容插入到cot_text
    outdata = []
    for (key,v) in alldata.items():
        text = v["text"] # 病历文书
        result = [f"{code}:{title}" for code,title in v["label_code_title"].items()]

        system_prompt = '''根据输入的病历文本，和提取到的ICD10编码结果，给出合理的解释，即：为什么当前病人的编码结果是这样的'''
        user_input = f"这是病历文书：{text}\n\n这是ICD10的编码结果：{result}"
        # cot_result = get_doubao(system_prompt, user_input)
        cot_result = ""

        line = {
            "instruction":"You are a coder who is good at ICD10. Now, based on the medical record text, extract the diagnosis and output the coding reason and coding result. The coding result is output in the format of code:name\n.",
            "input":text,
            "cot":cot_result,
            "output":"\n".join(result),
            "caseid":key
        }

        outdata.append(line)

    # 指定输出文件的路径
    output_file_path = "llm_train_data.json"
    # 将列表以 JSON 格式写入文件
    with open(output_file_path, "w", encoding="utf-8") as file:
        # 使用 json.dump 将数据写入文件
        json.dump(outdata, file, ensure_ascii=False, indent=4)
    print(f"数据已成功写入到文件：{output_file_path}")



# get_model_train_data()

def insert_labels():
    df = pd.read_csv("label_concat.csv", nrows=None)
    print(df)


    alldata = {}
    for i in range(df.shape[0]):
    #     # print(i)
        subject_id, hadm_id, icd_code, long_title = df.loc[i,['subject_id', 'hadm_id','icd_code', 'long_title']]
        # print(subject_id, hadm_id, seq_num, icd_code, long_title)
        key = str(subject_id) + "_" + str(hadm_id)
        if key not in alldata.keys():
            alldata[key] = {
                'subject_id': subject_id,
                "hadm_id":hadm_id,
                "label_code_title":{}
            }
        alldata[key]['label_code_title'][icd_code]=long_title


    # 整个内容插入到cot_text
    outdata = []
    for (key,v) in alldata.items():

        result = [f"{code}:{title}" for code,title in v["label_code_title"].items()]

        line = {
            "subject_id":v["subject_id"],
            "hadm_id":v["hadm_id"],
            "output":"\n".join(result)
        }
        # line["output"] = line["output"].replace("\'","\"")
        outdata.append(line)

    # 指定输出文件的路径
    output_file_path = "llm_train_data_labels.csv"
    pd.DataFrame(outdata).to_csv(output_file_path, index=False)

insert_labels()

# def get_cot_text_by_keys():
#
#     df = pd.read_csv("label_concat.csv",nrows=100)
#     print(df)
#
#     allkeys = set()
#     for i in range(df.shape[0]):
#         subject_id,hadm_id = df.loc[i,["subject_id","hadm_id"]]
#         key = str(subject_id) + "_" + str(hadm_id)
#         allkeys.add(key)
#
#     # df["subject_id"]
#
#     print(allkeys)
#
#     subjectids = ",".join([x.split('_')[0] for x in list(allkeys)])
#     hadmids = ",".join([x.split('_')[1] for x in list(allkeys)])
#     print(subjectids)
#     print(hadmids)
#
#     sql = '''
#     select d.subject_id,
#         d.hadm_id,
#         d.text as ttext
#     from mimic4.discharge d
#     where d.subject_id in ({}) and d.hadm_id in ({})
#     ;
#     '''.format(subjectids,hadmids)
#     print(sql)
#     alltext = db.query(sql)
#     print(alltext)

db.close()










