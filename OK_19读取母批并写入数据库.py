# -*- coding: UTF-8 -*-
# coding=utf-8
import os
import sys
import time
import datetime
import numpy as np
import pandas as pd
import pymssql
import configparser
import win32file
import shutil
from sqlalchemy import create_engine
# import sendmail as sendmail
# from pymssql import _mssql
# from pymssql import _pymssql
# import uuid
# import decimal
# from openpyxl import load_workbook
# import sys
# import traceback
# import smtplib
# from email.mime.text import MIMEText
# import subprocess
# import openpyxl
# from collections import Counter
# import pdb

start = time.time()

# if os.path.exists("result.txt"):
#     os.remove("result.txt")

if os.path.isfile("result.txt"):
    os.remove("result.txt")

output_file = open("result.txt",'a')
sys.stdout = output_file


def now_filename():
    # now_time = time.strftime('_%Y%m%d%H%M%S_', time.localtime())
    # now_us = str(datetime.datetime.now().microsecond)  # 获取的不是毫秒，而是us
    return datetime.datetime.now().strftime('_%Y%m%d%H%M%S_%f')
# 密码加解密
# def encrypt(key, plaintext):
#     key = key.lower()
#     keyList = []
#     fun_return=''
#     # 先将密钥转化为偏移的位数，然后加入到列表中
#     for i in key:
#         keyList.append(ord(i) - 97)
#     for i in range(0, len(plaintext)):
#         # i%len(keyList)实现了对密钥的重复利用
#         fun_return += chr(ord(plaintext[i]) + keyList[i % len(keyList)])
#         # print(chr(ord(plaintext[i]) + keyList[i % len(keyList)]), end='')
#     return fun_return


def decrypt(key, cipher):
    key = key.lower()
    key_list = []
    fun_return=''
    for i in key:
        key_list.append(ord(i) - 97)
    for i in range(0, len(cipher)):
        fun_return += chr(ord(cipher[i]) - key_list[i % len(key_list)])
        # print(chr(ord(cipher[i]) - key_list[i % len(key_list)]), end='')
    return fun_return


# 配置文件
# 实例化configParser对象
config = configparser.ConfigParser()
configPath = "config.ini"
# -read读取ini文件
config.read(configPath, encoding='GB18030')
host = config.get('info', 'host')
user = config.get('info', 'user')
password = decrypt('yefangjun',config.get('info', 'password'))
database = config.get('info', 'database')
etching_table= config.get('info', 'etching_table')
charset = config.get('info', 'charset')
path = config.get('info', 'path')
aoiadipath = config.get('info', 'aoiadipath')
MasterBatchPath = config.get('info', 'MasterBatchPath')
intervalTime = config.get('info', 'intervalTime')
hostip = config.get('info', 'sharehost')

# 在导出文件目录下建立两个文件夹:FINISHED
finishedPath = path + "\\" + "FINISHED"
DefectStaticPath = path + "\\" + "DefectStatic"
# print(finishedPath)

# 创建成功和失败两个文件夹
if not os.path.exists(finishedPath):
    os.mkdir(finishedPath)
    print("finishedPath folder Folder has been added")
print("finishedPath folder already exists")
if not os.path.exists(DefectStaticPath):
    os.mkdir(DefectStaticPath)
    print("DefectStaticPath Folder has been added")
print("DefectStaticPath folder already exists")


# print(os.listdir(path))

files = os.listdir(MasterBatchPath)
print("MasterBatch folder list are as follows：")
print(files)
print("="*20)
# 遍历所有文件
i = 0
j = 0
filenames = ""
result1 = ""
dict = {}
# setList=set()

print("Ready to connect to the database…………")
try:
    conn = pymssql.connect(host=host, user=user, password=password, database=database, charset=charset)
    cur = conn.cursor()
    print('Database connection succeeded！')
    # print(' ')
# except pymssql.OperationalError:
#    print('数据库连接错误！')
except Exception as result:
    print("Database connection error：%s" % result)
    
# table = Run次信息
# etching_table= 刻蚀信息录入
# select_sql = 'select 盒号片位信息,刻蚀信息 from 盒号片位信息 where 盒号片位信息=' + '\'' + hehaoinfo + '\''
# select_sql = 'select 盒号片位,刻蚀信息 from 盒号片位信息'
print("start query ecetching_info table processing to match……")
select_sql = 'select 盒号片位,刻蚀信息 from ' + etching_table
cur.execute(select_sql)
result = cur.fetchall()

columns_name = [tuple[0] for tuple in cur.description]
datafm = pd.DataFrame(result, columns=columns_name)
cur.close()  # 关闭游标
conn.close()  # 关闭数据库连接
print("刻蚀信息录入表查询完毕，获取的数据集为：\n%s"%datafm.head())
print("开始循环母批文件夹")
for file in files:
    # 判断文件是不是csv文件
    if file.split('.')[-1] in ['csv']:
        flag=win32file.GetFileAttributesW(MasterBatchPath + '\\' + file)
        flag=flag & 2
        if flag !=0:
            continue
        i += 1
        # 避免重名
        filename = file.split('.')[0]
        print("开始读取母批文件%s"%file)
        file_mupi = pd.read_csv(MasterBatchPath + "\\" + file , header=None,encoding='utf-8')
        np_FileMupi = np.array(file_mupi)
        # print(np_FileMupi)
        cellMupi = np_FileMupi[0][0]

        cellRow = 1
        colBoxno = np_FileMupi[cellRow][1]
        #复制对应AOIADI具体文件夹下面的efectStatic到指定目录中，再读取，最后再删除
        aoiadipath01 = aoiadipath + "\\" + cellMupi + "\DefectStatic_" + cellMupi + ".csv"
        DefectStaticCopy=DefectStaticPath+ "\DefectStatic_" + cellMupi + ".csv"
        shutil.copyfile(aoiadipath01, DefectStaticCopy)
        DefectStatic = pd.read_csv(DefectStaticCopy, encoding='UTF-16', sep='\t', na_filter=False)
        DefectStatic.insert(0,column = "条件列",value = DefectStatic['LotID']+DefectStatic['Out CST'])
        DefectStatic['path']=None
        DefectStatic['来料盒号']=None
        DefectStatic['黄光路径']=None
        DefectStatic['Marking_No2']=None


        # maxrow=max(DefectStatic['Count'])

        # 开始循环
        # while (colBoxno==None) or (colBoxno=='over'):
            # hehaoinfo=colBoxno +
        inpos=DefectStatic['in_pos']
        celltemp=inpos[0].split('_')[0]
        for cell in inpos:
            cellsp=cell.split('_')
            hehaoinfo = colBoxno + '_' + cellsp[1]
            hehaoinfo=hehaoinfo.replace('-', '')
            cellplace=DefectStatic[DefectStatic['in_pos'] == cell].index.tolist()[0]

            if cellsp[0] != celltemp:
                cellRow = cellRow + 1
                colBoxno = np_FileMupi[cellRow][1]
                hehaoinfo = colBoxno + '_' + cellsp[1]
                hehaoinfo=hehaoinfo.replace('-', '')
                # print(DefectStatic[DefectStatic['in_pos']==cell].index.tolist()[0])
            celltemp = cellsp[0]

            keshi=datafm[datafm['盒号片位']==hehaoinfo]['刻蚀信息'].iloc[0]
            DefectStatic.loc[cellplace, 'Marking_No2'] = keshi
            # DefectStatic.loc[cellplace,'OCR']=keshi[19:27]

        # config.set("file","rowscount",str(max(DefectStatic['Count'])))
        # fh=open('config.ini', 'w')
        # config.write(fh)
        # fh.close()
        print("开始导入数据")
        conn_engine = create_engine('mssql+pymssql://User_aoiadi_zhicheng_realtime_InserAlter:Zhecheng_1357924680@172.16.200.30/AOIADI')
        DefectStatic.to_sql('zhicheng_realtime', conn_engine, if_exists='append', index=False)
        conn_engine.dispose()
        print("导入数据成功")

        maxRow = len(np_FileMupi) - 1
        if np_FileMupi[maxRow][0] == 'over':
            # print(file)
            shutil.move(MasterBatchPath + "\\" + file, finishedPath + "\\")
            print("母批表已Over，已移走")
        os.remove(DefectStaticCopy)
        print("已删除复制出来的AOIADI源表")

end = time.time()
print('运行时间: %s 秒' % (end - start))
output_file.close()