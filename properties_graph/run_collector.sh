#!/usr/bin/env bash

hadoop fs -rm -r hdfs:///user/braemy/wikidata.json
hadoop fs -rm -r hdfs:///user/braemy/wikidata_test.json
spark-submit --master yarn --num-executors 200 \
 --executor-memory 4G \
 --conf spark.yarn.executor.memoryOverhead=1024 convert_to_parquet.py
 #--conf spark.driver.memory=4096 \


