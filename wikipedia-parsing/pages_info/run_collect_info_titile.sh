#!/usr/bin/env bash


#remove the output in hdfs in case it already exists
hadoop fs -rm -r hdfs:///user/braemy/wpTitle_to_wpId.json/


spark-submit --master yarn --num-executors 150 --executor-memory 4G --conf spark.yarn.executor.memoryOverhead=1024 collect_info_title.py

#remove the output file in the file system
rm -rf /dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId
#get the data from hdfs to file sysmte
hadoop fs -get hdfs:///user/braemy/wpTitle_to_wpId.json/ /dlabdata1/braemy/wikipedia_classification/wpTitle_to_wpId
# convert the data
python3 convert_to_dict.py