# rbdbigbackup

用于备份超大的rbd的



基于本地的数据库实现增量备份



```bas
[root@lab101 rbdbigbackup]# python rbdbigbackup.py rbd/test
Usage : rbdbigbackup.py [-h] [poolname/imagename|prifixname]  [command]
Ceph export tools - Version 1.0
OPTIONS
========
    -h          Print help
COMMANDS
=========
    --------
   | init |
    --------
    [poolname/imagename]      init                  初始化本地数据库（重新全量备份的时候也可以执行）
    --------
   | update |
    --------
    [poolname/imagename]      update                更新数据库中需要增量备份的对象
    --------
   | get |
    --------
    [poolname/imagename]      get                  下载增量的部分
    --------
   | fstrim |
    --------
    [poolname/imagename]       fstrim               清理远端已经删除的对象到目录fstrim
    --------
   | build |
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
```

