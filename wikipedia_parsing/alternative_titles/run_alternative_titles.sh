#!/usr/bin/env bash

#remove the output in hdfs in case it already exists
hadoop fs -rm -r hdfs:///user/braemy/alternative_titles.json
#run the tokenization
#if [ "$#" -eq 2 ];
#then
#    spark-submit --master yarn --num-executors 150 --executor-memory 2G --conf spark.yarn.executor.memoryOverhead=1024 alternative_titles.py --id_max $1 --subpart $2
#else
#    spark-submit --master yarn --num-executors 150 --executor-memory 2G --conf spark.yarn.executor.memoryOverhead=1024 alternative_titles.py --id_max $1
#fi

spark-submit --master yarn --num-executors 150 --executor-memory 2G --conf spark.yarn.executor.memoryOverhead=1024 alternative_titles.py
#remove the output file in the file system
rm -rf /dlabdata1/braemy/wikidataNER/alternative_titles/
#get the data from hdfs to file sysmte
hadoop fs -get hdfs:///user/braemy/alternative_titles.json/ /dlabdata1/braemy/wikidataNER/alternative_titles
# convert the data to conll format
#if [ "$#" -eq 2 ];
#then
#    python3 convert_to_dict.py --id $1 --subpart $2
#else
#    python3 convert_to_dict.py --id $1
#fi

python3 convert_to_dict.py