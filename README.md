# POI
经纬度数据聚类，寻找用户的兴趣点


# 运行方式
spark-submit --master yarn --deploy-mode cluster --num-executors 400 --executor-memory 4G --executor-cores 2 poi_pyspark_dbscan_to_table.py 2018-10-01 zhangxiang

- 解释

1、第一个参数是时间，如2018-10-01，第二个参数应该是标签库的库名，这里我的是zhangxiang,要改为对应的库名；

