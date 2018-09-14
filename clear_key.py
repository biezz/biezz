# coding:utf-8


import sys
import redis
import time
import sqlite3
import re
import collections
import logging

logging.basicConfig(filename ='redis.log',level=logging.DEBUG,format='%(asctime)s %(filename)s[line:%(lineno)d] %(message)s',datefmt='%m/%d/%Y %H:%M:%S %p')
logging.info('beginning!')

key_set = set()
key_list = list()
sequence_key = list()
key_tuple = tuple()
pool = redis.ConnectionPool(host='192.168.174.128',port=6379,db=0)
r = redis.Redis(connection_pool=pool)
print( "占用内存:",r.info('memory')["used_memory_human"])
print("key的总个数:",r.dbsize())

def analysis():
    begin_time = time.time()
    line = 1
    conn = sqlite3.connect(r'E:\File\memory.db')
    c = conn.cursor()    
    #while 1<= line <= (endlines-1000):
    while 1<= line <= 202:
        cursor = c.execute("select (key) from memory limit 100 offset %s;" %line)
        line = line + 100
        print("读入行数：%s" %(line-1))
        results = cursor.fetchall()
        for row in results:
        #ret = re.findall(r'^[\D]',row[0])[0]
        #ret = re.findall(r'(.*)[_|:]\d{4}',row[0])[0]
        #ret = re.findall(r'[\D]*\d?\D*\d?\D*',row[0])[0]
            ret = re.findall(r'(.*)_?\D',row[0])[0]
            key_list.append(ret)
    key_set = set(key_list)
    print(key_set)
    print("集合元素一共有：",len(key_set))
    logging.info('set is done')
    
    for item in key_set:
        key_size = c.execute("select sum(size_in_bytes),count(*) from memory where key like '%s%%';"%item)
        key_size = key_size.fetchall()
        key_tuple = (key_size[0][0],key_size[0][1],item)
        sequence_key.append(key_tuple)
    zipped = sorted(sequence_key, key=lambda s:s[0], reverse=True)
    f = open(r'C:\Users\Administrator\Desktop\python\output.txt','a')
    for zipped_order in zipped:
        print(zipped_order)
        s = str(zipped_order)
        f.write(s)
        f.write('\n')
    end_time = time.time()
    print(str(end_time - begin_time))
    f.write(str(end_time - begin_time))
    f.close
    logging.info("match done!")
    conn.close()
        
analysis()

while True:
    conn = sqlite3.connect(r'E:\File\memory.db')
    c = conn.cursor()
    key_type = input('请输入要清理的key的类别：')
    key_select = c.execute("select * from memory where key like '%s%%' order by size_in_bytes;"%key_type)
    keys = 0
    for key in key_select:
        keys = keys + 1
        print(key)
    print("总共：%s个"%keys)
    conn.close()
    g = input("请输入要清理的key(按q退出程序):")
    if g == 'q':
        sys.exit()
    def delete_value():
        
        try:
            #哈希删除
            if (r.type(g)) == b'hash':
                while r.type(g) != b'none':
                    list_value = r.hscan(g,0,count=15)
                    count = 0
                    youbiao = list_value[0]
                    print(youbiao)
                    print (list_value[1])
                    for item in list_value[1]:
                        if count%3 == 0:
                            time.sleep(3)
                        item = item.decode('utf-8')
                        print (item)
                        r.hdel(g,item)
                        count = count + 1
                        
            #列表删除
            elif (r.type(g)) == b'list':
                while r.type(g) != b'none':
                    list_value = r.lrange(g,0,-1)
                    count = 0
                    for item in list_value:
                        if count%3 == 0:
                            time.sleep(3)
                        item = item.decode('utf-8')
                        print (item)
                        r.lpop(g)
                        count = count + 1
                
            #集合删除
            elif (r.type(g)) == b'set':
                while r.type(g) != b'none':
                    list_value = r.sscan(g,0)
                    count = 0
                    for item in list_value[1]:
                        if count%3 == 0:
                            time.sleep(3)
                        item = item.decode('utf-8')
                        print(item)
                        r.spop(g)
                        count = count + 1
                        
                        
                
            #有序集合删除
            elif (r.type(g)) == b'zset':
                while r.type(g) != b'none':
                    list_value = r.zscan(g,0)
                    count = 0
                    for item in list_value[1]:
                        if count%3 == 0:
                            time.sleep(3)
                        item = item[0].decode('utf-8')
                        print(item)
                        r.zrem(g,item)
                        count = count + 1
                    
            #字符串删除
            elif (r.type(g)) == b'string':
                r.delete(g)
            
            else:
                print("继续删除")
        
        except Exception as err:
            print('delete error!',err)
            
    delete_value()
    logging.info('finish')