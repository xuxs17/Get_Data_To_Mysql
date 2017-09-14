# -*- coding: utf-8 -*-
"""
Created on Fri Sep  8 12:01:29 2017

@author: changjin
"""
#加载相应模块
import pymysql
import queue
import threading
#import time


#获取txt数据的线程
class Get_Data(threading.Thread):
    
    def __init__(self,work_queue):
        super().__init__()
        self.work_queue = work_queue
        
    def run(self):
        
        #打开文本,采用读模式
        f = open("d:/XU/wantup/GLG_001.txt", "r")
        #第一行为无用数据,设置跳过第一行
        i = 0
        while True:            
            line = f.readline()
            i = i+1
            if i == 1:                
                continue
            if line:           
                #处理每行\n
                line = line.strip('\n')
                #分隔每行数据
                line = line.split('\t',6)
                #将处理完的一行数据放入队列work_queue中
                self.work_queue.put(line)
                #向队列中存数设置暂停
                #time.sleep(1)
            else:
                break
		    

#向mysql插入队列数据的线程 
class  Insert_Data(threading.Thread):
    
    def __init__(self,work_queue):
        super().__init__()
        self.work_queue = work_queue
        
    def run(self):
	   #连接mysql,以下是mysql参数
        conn = pymysql.connect(
                host='xxx.xxx.xxx',
                port=3306,
                user='xxx',
                passwd='xxx',
                db='test',
                charset='utf8',
            )
        #获取游标cursor
        cursor =conn.cursor()
        #建表前检查表名是否冲突
        cursor.execute("DROP TABLE IF EXISTS attendance") 
        #创建数据表SQL语句
        sql = """CREATE TABLE attendance (
                No  varchar(30) NOT NULL,
                TMNo varchar(30),
                EnNo varchar(30),  
                Name varchar(30),
                GMNo varchar(30),
                Mode varchar(30),
                Datetime varchar(30)
                )"""
        #执行sql语句
        cursor.execute(sql) 
        conn.commit()
        #将队列中的数据取出插入mysql中
        while True:
            line = self.work_queue.get()
            cursor.execute(
            "insert into attendance(No,TMNo,EnNo,GMNo,Mode,DateTime) values(%s,%s,%s,%s,%s,%s)",
            [line[0], line[1], line[2], line[4], line[5], line[6]])
            #从队列取数设置暂停
            #time.sleep() 
            
		      
def main():

    #实例一个队列queue
    work_queue = queue.Queue()

    get_data = Get_Data(work_queue)

    #当主线程退出时子线程也退出
    get_data.daemon = True
    get_data.start()
    
    insert_data = Insert_Data(work_queue)

    #当主线程退出时子线程也退出
    insert_data.daemon = True
    insert_data.start()
    
    work_queue.join()
    
    
if __name__ == '__main__':
    
    main()
    
    

   
