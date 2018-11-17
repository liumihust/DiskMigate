## Disk Migration Tool For HDFS
### 1 Introduction
Recently, I meet a scenario where HDFS users want to migrate all the data of the old volumes to newly added volumes. Although HDFS now has a DiskBalancer tool, but it dosen't meet the requirement of us. So, we develop a new tool DiskMigration, which can migrate all the data in the current volumes to the new volumes and keep balance of data distribution at the same time. As follows:
![hdfs11](https://github.com/liumihust/gitTset/blob/master/hdfs11.PNG)
The result our tool can get: migrate & balance      

![hdfs11](https://github.com/liumihust/gitTset/blob/master/hdfs12.PNG)
### 2 Data Distribution
We try to decide the data quota for new volumes by the their capacity, and distribute it by multi steps. 
![hdfs11](https://github.com/liumihust/gitTset/blob/master/hdfs21.PNG)
![hdfs11](https://github.com/liumihust/gitTset/blob/master/hdfs22.PNG)
### 3 How To Use It
You only need to replace the hdfs.jar and hdfs-client.jar of you HDFS cluster.
```
hdfs diskbalancer -plan node1 -type diskMigrate
```
will create the migrate plan, and save it as json in HDFS:/system/diskbalancer/2017-Jul-25-13-46-17/node1.plan.json, for example.
```
hdfs diskbalancer -execute /system/diskbalancer/2017-Jul-25-13-46-17/node1.plan.json
```
will process background, you can also query its processing status by:
```
hdfs diskbalancer -query node1:9867
```
If it outputs 'Result: PLAN_DONE', the migrate has finished. The result can be showed by linux commandline:
```
df
```
### 4 Experiments Result on Aliyun ECS
#### 4.1 Before Migrate
There are two new empty disks
```
[root@node1 ~]# df
Filesystem     1K-blocks     Used Available Use% Mounted on
/dev/vda1       41152832 14576524  24479208  38% /
tmpfs            4030552        0   4030552   0% /dev/shm
/dev/vdb       103081248    62540  97775828   1% /tmp/hadoop1
/dev/vdc       103081248  4849888  92988480   5% /tmp/hadoop2
/dev/vdd       103081248    61112  97777256   1% /tmp/hadoop3
```
#### 4.2 After Migrate
All the data of old disk has been migrated to new disks.
```
[root@node1 ~]# df
Filesystem     1K-blocks     Used Available Use% Mounted on
/dev/vda1       41152832 14576528  24479204  38% /
tmpfs            4030552        0   4030552   0% /dev/shm
/dev/vdb       103081248  2325524  95512844   3% /tmp/hadoop1
/dev/vdc       103081248    61116  97777252   1% /tmp/hadoop2
/dev/vdd       103081248  2587360  95251008   3% /tmp/hadoop3
```
### Detail
https://github.com/liumihust/ecs.hadoop/blob/master/DiskMigrationForHDFS.md
