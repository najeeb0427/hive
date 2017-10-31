#!/bin/bash

d=`echo $(date +%Y-%m-%d-%H-%M-%s`
url="http://pathinput"
directory_name=<directory_name>
hdfs_path="/user/data/"

mkdir ${d}
cd ${d}
wget url
unzip ${d}.zip
hadoop fs -mkdir ${hdfs_path}/${d}
hadoop fs -put *.csv ${hdfs_path}/${d}

hive -hiveconf path=${hdfs_path}/${d} -f hive_script.hql

----------------------------

hive_script.hql
-----------------------------------------

LOAD DATA INPATH ${hiveconf:path}/* into table <tablename>
--rest of hive script------
