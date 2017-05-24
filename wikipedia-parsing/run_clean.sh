#!/usr/bin/env bash

#remove the output in hdfs in case it already exists
hadoop fs -rm -r hdfs:///user/braemy/wikipedia_cleaned_$1$2.parquet
#run the tokenization
spark-submit --master yarn --num-executors 150 --executor-memory 4G --conf spark.yarn.executor.memoryOverhead=2048 clean_wikipedia.py --id_max $1 --subpart $2
