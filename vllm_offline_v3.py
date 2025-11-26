import pandas as pd
import json
import csv
from tqdm import tqdm
import pandas as pd
import datasets
from util_db import DBApi
from torch.utils.data import DataLoader
import sys


def icd10():
    db = DBApi()
    
    # def get_model_train_data():
    sql = '''
        select d.subject_id,d.hadm_id,d.`text`, la.output as label
        from mimic4.llm_train_data_labels la
        join mimic4.discharge d 
        on la.subject_id = d.subject_id and la.hadm_id = d.hadm_id
        where d.`text` is not null
        limit 20000,30000;
    ;
    '''
    sql = '''
        select d.subject_id,d.hadm_id,d.`text`, la.output as label
        from mimic4.llm_train_data_labels la
        join mimic4.discharge d 
        on la.subject_id = d.subject_id and la.hadm_id = d.hadm_id
        where d.`text` is not null and la.cot is null
    ;
    '''

    print("here")
    sql = '''
        select la.subject_id,la.hadm_id,la.text, la.output as label
        from mimic4.llm_train_data_labels la
        where la.text is not null and la.cot is null
        limit 10
        ;
    '''
    
    data = db.query(sql)
    df = pd.DataFrame.from_records(data)
    print(df)
    df.columns = ['subject_id', 'hadm_id', 'text', 'label']
    ds = datasets.Dataset.from_pandas(df)
    print(ds)
    size = ds.__len__()
    print(f"Size of dataset: {size} prompts")

    # model_path = r'E:\pycharm\com_gitlab\lightning2025\_pltokenizers\Qwen\Qwen2.5-5M-Instruct'
    #model_path = "/supercloud/llm-data/hugmodels/modelscope/ddd/Qwen/Qwen2.5-72B-Instruct"
    #model_path = "/supercloud/llm-data/hugmodels/modelscope/ddd/Qwen/Qwen2.5-32B-Instruct"
    #model_path = "/supercloud/llm-data/hugmodels/modelscope/ddd/Qwen/Qwen2.5-3B-Instruct"
    #model_path = "/supercloud/llm-data/hugmodels/modelscope/ddd/Qwen/Qwen2.5-3B-Instruct"
    #tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True, use_fast=False,
    
    #user_input = "You are a coder who is proficient in ICD10. Now, please analyze and give the reason for the coding based on the medical record text and ICD10 coding results. The output content is output in the format of 'icd10code:reason'."
    #user_input = '''You are a coder who is proficient in ICD10. Now, please analyze and give the reason for the coding based on the medical record text and ICD10 coding results.
    #Mainly analyze the reasons for such coding results and follow the evidence in the original medical record. If the chain of evidence is incomplete, there is no need to output the analysis corresponding to a certain coding.
    #The output content is output in the form of "icd10code:reason".
    #'''
    user_input='''You are a coder who is proficient in ICD10. Now, please analyze and give reasons for the coding based on the medical record text and ICD10 coding results.
    The reasons for such coding should be mainly given in combination with the evidence in the original medical record.
    For codes that may not be applicable, do not output the corresponding reasons and do not perform additional analysis.
    The output content is output in the form of "icd10code:reason".
    '''
    
    def map_func(x):
        msg = [
            {"role": "user", "content": user_input},
            {"role": "assistant", "content": "Here is the medical record text:" 
                                + x['text']
                                + "\n\nHere is the coding results:" + x['label']
                                + "\n\nPlease output the coding reason:"
                }
        ]
        x["input"] = msg 
        return x
    
    print(ds)
    ds = ds.map(map_func)
    print(ds)
    
    # Generate texts from the prompts. The output is a list of RequestOutput
    # objects that contain the prompt, generated text, and other information.
    outloader = DataLoader(ds, batch_size=40)

    for idx,data in enumerate(outloader):
        #print(data["input"])
        print(len(data["input"]))
        if idx > 5: break
    return
    
    with open('./doubao_cot_all.csv','w', newline='', encoding='utf-8') as csvfile:
        # df.columns = ['subject_id', 'hadm_id', 'text', 'label'] # text转成了input（加上了prompt的）
    
        # 定义 CSV 文件的列名
        fieldnames = ['subject_id', 'hadm_id', 'label', 'text', 'cot', 'instruction']
        # input是带有 chat_template 的病历文书信息 
    
        # 创建 CSV 写入器
        #writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quotechar='"', escapechar='\\')    
    
        # 写入列名
        writer.writeheader()
    
        # 遍历 DataLoader 中的每个批次
        for idx, samples in enumerate(outloader):
            # df.columns = ['subject_id', 'hadm_id', 'text', 'label']
            # 当前的数据包含的列
            #    df.columns = ['subject_id', 'hadm_id', 'text', 'label'] + prompt
            sids = samples["subject_id"]
            hids = samples["hadm_id"]
            prompts = samples["input"] # apply_chat_template的结果
            labels = samples["label"] # icdcode:title\n 的格式
            texts = samples['text'] # 病历文书

            # print(prompts)
            outputs = llm.generate(prompts, sampling_params)
            # Print the outputs.
            if idx % 50 == 0:
                print(idx, "-" * 30)
            # for sub_idx, output in enumerate(outputs):
            for sid, hid, output, label, text in zip(sids, hids, outputs, labels, texts):
                #print(output.outputs[0].text)
                writer.writerow({
                    'subject_id': sid,
                    'hadm_id': hid,
                    'label': label,
                    'instruction':user_input, # 记录
                    'text':text,
                    # 'input': output.prompt.replace("\'","\""), # 没必要再生成了
                    'cot': output.outputs[0].text.replace("\'","\"") # 生成的cot的结果

                })
            csvfile.flush()
    
icd10()
