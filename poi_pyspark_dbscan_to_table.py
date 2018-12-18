# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 10:11:59 2018

@author: zhangxiang
"""



#import sys
#reload(sys)
#sys.setdefaultencoding("utf-8")

import importlib,sys
#importlib.reload(sys)
sys.path.append("/opt/spark/spark-2.2.0-bin-hadoop2.6/python")
sys.path.append("/opt/spark/spark-2.2.0-bin-hadoop2.6/python/lib/py4j-0.10.4-src.zip")

import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from pandas.core.frame import DataFrame
import argparse

# 指定集群python路径(yarn模式需要)
import os
os.environ["PYSPARK_PYTHON"]="/data/anaconda3/anaconda3/bin/python"

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf 
from pyspark.sql import HiveContext


# 计算每一个uid的地理位置聚类lables (采用dbscan算法)
def get_lables_partitions(x):
    final_iterator = []
    for xi in x:
        ary = []
        for _row in xi[1]:
            #t = [v for k,v in items() if k in ['pid', 'lon', 'lat']]
            t = _row.asDict()
            #print(t)
            #print(type(t))
            pid = t['pid']
            login = t['login']
            logout = t['logout']
            lon = t['lon']
            lat = t['lat']
            weekday_weekend = t['weekday_weekend']
            ary.append([pid, login, logout, lon, lat, weekday_weekend])

        df = pd.DataFrame(ary)
        df.columns = ['pid', 'login', 'logout', 'lon', 'lat', 'weekday_weekend']
        dftmp = df[['lon', 'lat']]
        db = DBSCAN(eps=eps, min_samples=min_samples, algorithm="ball_tree", metric='haversine').fit(np.radians(dftmp))
        df['labels'] = db.labels_
        #final_iterator.append(v for v in df.as_matrix().tolist() ] )  
        final_iterator.append(df.as_matrix().tolist())
    return iter(final_iterator)

def get_lables(x):
    ary = []
    for _row in x[1]:
        #t = [v for k,v in items() if k in ['pid', 'lon', 'lat']]
        t = _row.asDict()
        #print(t)
        #print(type(t))
        pid = t['pid']
        login = t['login']
        logout = t['logout']
        lon = t['lon']
        lat = t['lat']
        weekday_weekend = t['weekday_weekend']
        ary.append([pid, login, logout, lon, lat, weekday_weekend])

    df = pd.DataFrame(ary)
    df.columns = ['pid', 'login', 'logout', 'lon', 'lat', 'weekday_weekend']
    dftmp = df[['lon', 'lat']]

    db = DBSCAN(eps=eps, min_samples=min_samples, algorithm="ball_tree", metric='haversine').fit(np.radians(dftmp))

    df['labels'] = db.labels_

    return df.as_matrix().tolist()

# 将[[[1,2],[2,3]],[[2,3],[4,5],[6,8]]] 转换成 [[1,2],[2,3],[2,3],[4,5],[6,8]]
def split_list(x):
    return [v for v in x]    


if __name__ == '__main__':

    distance = 0.8
    min_samples = 2
    date_time = str(sys.argv[1])
    warehouse_name = str(sys.argv[2])

    table_name = warehouse_name + '.' + 'sctdwd_bill_deviceinfo_offline_dbscan_d'
    kms_per_radian = 6371.0088
    eps = distance / kms_per_radian

    if isinstance(float(distance), float) and isinstance(int(min_samples), int):

        spark = SparkSession.builder \
            .master("yarn") \
            .appName("poi") \
            .config("spark.some.config.option", "some-value") \
            .enableHiveSupport() \
            .getOrCreate() \
       
        res = spark.sql("                                                                                     \
            select                                                                                            \
                pid                                                                                           \
                ,login                                                                                        \
                ,logout                                                                                       \
                ,gps_lon lon                                                                                  \
                ,gps_lat lat                                                                                  \
                ,if(pmod(datediff(pt_dt,'2012-01-01'),7) in (0,6) , 'weekend', 'weekday' ) weekday_weekend    \
            from security_dwd.sctdwd_bill_deviceinfo_offline_d                                                \
            where                                                                                             \
                pt_dt > date_add( '%s' ,-30) and pt_dt <= '%s'                                               \
                and ( gps_lat != 0 and gps_lat is not null and gps_lat > -90 and gps_lat < 90   )             \
                and ( gps_lon != 0 and gps_lon is not null and gps_lon > -180 and gps_lon < 180 )             \
                and ( login is not null                                                         )             \
                and ( logout is not null                                                        )             \
                and ( logout > login                                                            )             \
        " % (date_time,date_time))
        res_t1 = res.rdd.map(lambda x: ((x[0],x[5]), x))
        res_t2 = res_t1.groupByKey().mapValues(list)
        res_t3 = res_t2.map(get_lables)
#        res_t3 = res_t2.mapPartitions(get_lables_partitions)
        res_t4 = res_t3.flatMap(split_list)
        res_t5 = spark.createDataFrame(res_t4,["pid","login","logout","lon","lat","weekday_weekend","labels"]).repartition(50).createOrReplaceTempView("df")
 
        spark.sql(""" drop table if exists %s """ % table_name)
        spark.sql("create table if not exists %s \
            (pid INT,\
                login STRING,\
                logout STRING,\
                lon FLOAT,\
                lat FLOAT,\
                weekday_weekend STRING,\
                labels INT) PARTITIONED BY (pt_dt STRING)" % table_name)
        spark.sql("SET spark.sql.shuffle.partitions=300")
        spark.sql("INSERT OVERWRITE TABLE %s PARTITION(pt_dt= '%s')\
            select   \
                cast(pid as int) pid, \
                cast(login as string) login,\
                cast(logout as string) logout,\
                cast(lon as float) lon,\
                cast(lat as float) lat,\
                cast(weekday_weekend as string) weekday_weekend,\
                cast(labels as int) labels \
            from df\
        " % (table_name,date_time))

        spark.stop()
