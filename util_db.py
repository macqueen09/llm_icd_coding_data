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
                'charset': 'utf8mb4'}  # Database configuration information

        self.db = pymysql.connect(**config)
        self.cursor = self.db.cursor()

    def query(self,sql,fetch_one=False):
        try:
            self.cursor.execute(sql)
            # Retrieve the query results
            if fetch_one:
                result = self.cursor.fetchone()
            else:
                result = self.cursor.fetchall()
            # Submit changes
            self.db.commit()
        except Exception as e:
            print(f"Error: {e}")
            print(sql)
            result = None
        return result

    def insert(self,sql):
        try:
            # Execute the SQL query
            self.cursor.execute(sql)
            # Execute the SQL query
            self.db.commit()
        except Exception as e:
            print("Error",sql,e)
            # Rollback in case of an 
            self.db.rollback()
            return e
        return "success"

    def insert_many(self,sql,values):
        # try:
        self.cursor.executemany(sql, values)
        self.db.commit()

    def insert_item(self,sql,item):
        # sql = "insert into test2(url, time) values(%s,%s)"  # # Note the difference here compared to the previous form
        # par = (Url，Time)
        try:
            self.db.execute(sql, item)
            self.db.commit()  # # Commit to the database for execution, make sure to commit
        except Exception:
            self.db.rollback()  # Rollback in case of an error


    def close(self):
        self.cursor.close()
        self.db.close()

if __name__ == "__main__":
    db = DBApi()
    sql = ''' SELECT * FROM mimic3.ADMISSIONS limit 10; '''
    data = db.query(sql)
    print(data)
    db.close()
