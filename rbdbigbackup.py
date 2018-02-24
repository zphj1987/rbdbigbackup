#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import sys
import commands
import json
import sqlite3
import time

def main():
    if len(sys.argv) == 1 or len(sys.argv) == 2:  
        sys.argv.append("-h")  
    if sys.argv[1] == '-h' or sys.argv[1] == 'help':
       help()
    if len(sys.argv) == 3 :
        if sys.argv[2] == 'init':
            checkoutput=checkpoolimage(sys.argv[1])
            if checkoutput[4] ==1:
                poolname=checkoutput[0]
                imagename=checkoutput[1]
                prifixname=checkoutput[2].split('.',1)[1]
                objects=checkoutput[3]
                init_image_localdb(poolname,imagename,prifixname,objects)
            else:
                print "check your pool/image name!"
        elif sys.argv[2] == 'update':
            checkoutput=checkpoolimage(sys.argv[1])
            if checkoutput[4] ==1:
                poolname=checkoutput[0]
                imagename=checkoutput[1]
                prifixname=checkoutput[2].split('.',1)[1]
                objects=checkoutput[3]
                update_image_localdb(poolname,imagename,prifixname,objects)        

        elif sys.argv[2] == 'get':
            checkoutput=checkpoolimage(sys.argv[1])
            if checkoutput[4] ==1:
                poolname=checkoutput[0]
                imagename=checkoutput[1]
                prifixname=checkoutput[2].split('.',1)[1]
                objects=checkoutput[3]
                get_image_localdb(poolname,imagename,prifixname,objects)        

        elif sys.argv[2] == 'fstrim':
            checkoutput=checkpoolimage(sys.argv[1])
            if checkoutput[4] ==1:
                poolname=checkoutput[0]
                imagename=checkoutput[1]
                prifixname=checkoutput[2].split('.',1)[1]
                objects=checkoutput[3]
                fstrim_image_localdb(poolname,imagename,prifixname,objects)   


        elif sys.argv[2] == 'build':
            prifixname=sys.argv[1]
            checkdbifexit(prifixname)
            build_image_localdb(prifixname)
        else:
            help()

def processshow(num,objects):
    print "当前的进度:{0}/{1}   {2}%\r".format(num,objects,round((num + 1) * 100 / objects)), 
    time.sleep(0.01)


#函数判断为poolname/imagename组合的形式,判断包含字符串，然后以字符串为分割得到存储池名称和image名称，否则说没有
def checkpoolimage(name):
    if name.count("/") == 1:
        if len(name.split('/',1)[0])!=0 and len(name.split('/',1)[1])!=0:
            poolname = name.split('/',1)[0]
            imagename = name.split('/',1)[1]
            rbd_info = commands.getoutput('rbd info %s/%s --format json --pretty-format 2>/dev/null'%(poolname,imagename) )
            try:
                json_str=json.loads(rbd_info)
                rbd_prifix=json_str['block_name_prefix']
                objects=json_str['objects']
                return [poolname,imagename,rbd_prifix,objects,1]
            except:
                return ['','','','',0]
        else:
            return ['','','','',0]
    else:
        return ['','','','',0]

#需要判断下本地数据库是否记录了这个rbd 的相关信息（正常应该有的）
def checkdbifexit(prifixname):
    conn = sqlite3.connect('rbd.db')
    c = conn.cursor()
    
#初始化数据库的表
def init_image_localdb(poolname,imagename,prifixname,objects):
    conn = sqlite3.connect('rbd.db')
    c = conn.cursor()
    try:
        c.execute("drop table [%s-%s-%s-%s]" %(poolname,imagename,prifixname,objects))
    except:
        pass
    c.execute("CREATE TABLE [%s-%s-%s-%s](objectnum PRIMARY KEY,objectname TEXT,getmtime,savemtime,ifget)" %(poolname,imagename,prifixname,objects))
    print "初始化本地数据库:"
    for num in range(objects):
        num_to_hex = commands.getoutput('printf "%.16x\n"' %num )
        c.execute("INSERT INTO [%s-%s-%s-%s](objectnum,objectname,getmtime,savemtime,ifget) VALUES(%s,'rbd_data.%s.%s',NULL,NULL,0)" %(poolname,imagename,prifixname,objects,num,prifixname,num_to_hex))
#####计时器技术
        processshow(num,objects)
####
        conn.commit()
    print ""
    print("本地数据库rbd.db生成数据库表: %s-%s-%s-%s" %(poolname,imagename,str(prifixname),objects) )
    conn.close()

def update_image_localdb(poolname,imagename,prifixname,objects):
    conn = sqlite3.connect('rbd.db')
    c = conn.cursor()

    print "更新本地数据库:"
    search=0
    update=0
    for num in range(objects):
        num_to_hex = commands.getoutput('printf "%.16x\n"' %num )
        getmtime=commands.getstatusoutput('rados stat -p %s rbd_data.%s.%s  2>/dev/null' %(poolname,prifixname,num_to_hex))
        if getmtime[0] == 0:
            search=search+1
            newgetmtime=getmtime[1].split(' ',-1)[2]+" "+getmtime[1].split(' ',-1)[3]
            c.execute("UPDATE [%s-%s-%s-%s] SET getmtime = '%s' WHERE objectnum = %s ;" %(poolname,imagename,prifixname,objects,newgetmtime,num))
        elif getmtime[0] != 0:
            c.execute("UPDATE [%s-%s-%s-%s] SET getmtime = NULL,ifget = '0' WHERE objectnum = %s ;" %(poolname,imagename,prifixname,objects,num))

        cursor=c.execute("SELECT  getmtime,savemtime FROM [%s-%s-%s-%s]  WHERE objectnum = %s ;" %(poolname,imagename,prifixname,objects,num))
        for row in cursor:
            if row[0] != row[1] and row[0] != None:
                c.execute("UPDATE [%s-%s-%s-%s] SET ifget = '1' WHERE objectnum = %s ;" %(poolname,imagename,prifixname,objects,num))
                update=update+1
        conn.commit()
        processshow(num,objects)
    print ""
    print("更新数据库rbd.db数据库表: %s-%s-%s-%s,本次查询对象数:%s,下次需要下载对象数:%s" %(poolname,imagename,str(prifixname),objects,search,update))
    conn.close()

def get_image_localdb(poolname,imagename,prifixname,objects):
    conn = sqlite3.connect('rbd.db')
    c = conn.cursor()
    try:
        os.makedirs("%s/%s" %(poolname,imagename))
    except:
        pass

    print "下载RBD的对象:"
    downid=1
    checkdown=c.execute("SELECT  COUNT(*)  FROM [%s-%s-%s-%s]  WHERE ifget=\"1\" ;" %(poolname,imagename,prifixname,objects) )
    for num in checkdown:
        print "本次需要下载对象数目:",num[0]
        needdown=num[0]

    for num in range(objects):
        num_to_hex = commands.getoutput('printf "%.16x\n"' %num )
        cursor=c.execute("SELECT  ifget FROM [%s-%s-%s-%s]  WHERE objectnum = %s ;" %(poolname,imagename,prifixname,objects,num) )
        for row in cursor:
            if row[0] == '1':
                savemtime=commands.getstatusoutput('rados stat -p %s rbd_data.%s.%s |awk \'{print $3,$4}\' 2>/dev/null'%(poolname,prifixname,num_to_hex))
                getobject=commands.getstatusoutput('rados  -p %s get rbd_data.%s.%s %s/%s/rbd_data.%s.%s  2>/dev/null'%(poolname,prifixname,num_to_hex,poolname,imagename,prifixname,num_to_hex))                
                if savemtime[0] == 0 and getobject[0] == 0:
                    c.execute("UPDATE [%s-%s-%s-%s] SET ifget = '0',savemtime = '%s'  WHERE objectnum = %s ;" %(poolname,imagename,prifixname,objects,savemtime[1],num) )
                processshow(downid,needdown)
                downid=downid+1
        conn.commit()
    print ""        
    print poolname,imagename,"对象下载完成"
    conn.close()

def fstrim_image_localdb(poolname,imagename,prifixname,objects):
    conn = sqlite3.connect('rbd.db')
    c = conn.cursor()
    try:
        os.makedirs("%s/%s/fstrim" %(poolname,imagename))
    except:
        pass

    for num in range(objects):
        num_to_hex = commands.getoutput('printf "%.16x\n"' %num )
        cursor=c.execute("SELECT  getmtime  FROM [%s-%s-%s-%s]  WHERE objectnum = %s ;" %(poolname,imagename,prifixname,objects,num) )
        for row in cursor:
            if row[0] == None:
                mvfile=commands.getoutput('mv  %s/%s/rbd_data.%s.%s %s/%s/fstrim/  2>/dev/null'%(poolname,imagename,prifixname,num_to_hex,poolname,imagename))
        conn.commit()
    print ""        
    print poolname,imagename,"清理完成"
    conn.close()

def build_image_localdb(prifixname):
    conn = sqlite3.connect('rbd.db')
    c = conn.cursor()
    cursor=c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    print "拼接RBD块设备:"
    for row in cursor:
        dbprifixname=row[0].split('-',-1)[2]
        if dbprifixname == prifixname:
            dbpoolname=row[0].split('-',-1)[0]
            dbimagename=row[0].split('-',-1)[1]
            dbobjects=row[0].split('-',-1)[3]
            obj_size=4194304
            rebuild_block_size=512
            rbd_size=int(obj_size)*int(dbobjects)

            try:
                mklocalfile=commands.getoutput('dd if=/dev/zero of=%s/%s/%s bs=1 count=0 seek=%d 2>/dev/null'%(dbpoolname,dbimagename,dbimagename,rbd_size))
            except:
                print "检查本地的rbd路径对象是否存在"
            #print buildfile
            files = [f for f in os.listdir('./%s/%s/' %(dbpoolname,dbimagename)) if os.path.isfile("./%s/%s/%s" %(dbpoolname,dbimagename,f)) and prifixname in f] 
            filenum=len(files)
            for index,f in enumerate(files):
                getseek_loc=commands.getoutput('echo %s | awk -F_ \'{print $2}\' | awk -v os=%d -v rs=%d -F. \'{print os*strtonum("0x" $NF)/rs}\'' %(f,obj_size,rebuild_block_size) )
                dd_to_file=commands.getoutput( 'dd conv=notrunc if=%s/%s/%s of=%s/%s/%s seek=%s bs=%s 2>/dev/null' %(dbpoolname,dbimagename,f,dbpoolname,dbimagename,dbimagename,getseek_loc,rebuild_block_size))
                processshow(index,filenum)
            print ""    
            print "生成本地Image:",dbimagename

def help():    print """Usage : rbdbigbackup.py [-h] [poolname/imagename|prifixname]  [command]
Ceph export tools - Version 1.0
OPTIONS
========
    -h          Print help
COMMANDS
=========
    --------
   |\033[0;32;40m init\033[0m |
    --------
    [poolname/imagename]      init                  初始化本地数据库（重新全量备份的时候也可以执行）
    --------
   |\033[0;32;40m update\033[0m |
    --------
    [poolname/imagename]      update                更新数据库中需要增量备份的对象
    --------
   |\033[0;32;40m get\033[0m |
    --------
    [poolname/imagename]      get                  下载增量的部分
    --------
   |\033[0;32;40m fstrim\033[0m |
    --------
    [poolname/imagename]       fstrim               清理远端已经删除的对象到目录fstrim
    --------
   |\033[0;32;40m build\033[0m |
    --------
    [prifixname]              build                根据本地的对象的prifix对数据进行拼接
    --------
-----------------------------------------------------------------------
操作步骤：
[备份过程]
init 来初始化本地数据库  --->   update 定期更新记录需要备份的对象 ---> get 下载数据库中记录需要更新的数据 ---> fstrim 清理本地的垃圾数据 
[恢复过程]
build是恢复数据的时候使用的,testrbd为生成的本地image:
#losetup /dev/loop0 testrbd
#mount /dev/loop0 /mnt
#losetup -d /dev/loop0
----------------------------------------------------------------
语法例子:
#init#     rbdbigbackup.py  rbd/testrbd init 
#build#    rbdbigbackup.py   103b6b8b4567 build
"""

if __name__ == '__main__':
    main()
