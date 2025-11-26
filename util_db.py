# -*- coding: utf-8 -*-

import pymysql


'''
url: jdbc:mysql://172.20.0.173:3306/hpm_db?useUnicode=true&characterEncoding=UTF-8&useSSL=false&zeroDateTimeBehavior=convertToNull
username: root
password: Zhy778899
'''

class DBApi():
    def __init__(self):
        config = {'host': '192.168.17.171',
                'user': 'qwe',
                'passwd': '123',
                'port': 3306,
                'db':'mimic3',
                'charset': 'utf8mb4'}  # 数据库配置信息

        self.db = pymysql.connect(**config)
        self.cursor = self.db.cursor()

    def query(self,sql,fetch_one=False):
        try:
            self.cursor.execute(sql)
            # 获取查询结果
            if fetch_one:
                result = self.cursor.fetchone()
            else:
                result = self.cursor.fetchall()
            # 提交更改
            self.db.commit()
        except Exception as e:
            print(f"Error: {e}")
            print(sql)
            result = None
        return result

    def insert(self,sql):
        try:
            # 执行sql语句
            self.cursor.execute(sql)
            # 执行sql语句
            self.db.commit()
        except Exception as e:
            print("发生错误",sql,e)
            # 发生错误时回滚
            self.db.rollback()
            return e
        return "success"

    def insert_many(self,sql,values):
        # try:
        self.cursor.executemany(sql, values)
        self.db.commit()

    def insert_item(self,sql,item):
        # sql = "insert into test2(url, time) values(%s,%s)"  # 注意此处与前一种形式的不同
        # par = (Url，Time)
        try:
            self.db.execute(sql, item)
            self.db.commit()  # 提交到数据库执行，一定要记提交哦
        except Exception:
            self.db.rollback()  # 发生错误时回滚


    def close(self):
        self.cursor.close()
        self.db.close()

if __name__ == "__main__":
    db = DBApi()
    sql = ''' SELECT * FROM mimic3.ADMISSIONS limit 10; '''
    data = db.query(sql)
    print(data)
    db.close()
