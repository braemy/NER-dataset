#!/usr/bin/env bash

#remove the output in hdfs in case it already exists
hadoop fs -rm -r hdfs:///user/braemy/$1/wikipedia_cleaned.parquet
#run the cleaning
if [ "$#" -eq 1 ];
then
    spark-submit --master yarn --num-executors 150 --executor-memory 2G --conf spark.yarn.executor.memoryOverhead=1024 clean_wikipedia.py --language $1
else
    echo "you need to pass a language (en, de)"
fi
#sh run_check_cleaning.sh $1

# Alternative titles
#rm -rf /dlabdata1/braemy/wikidataNER/alternative_titles2/
#get the data from hdfs to file sysmte
#hadoop fs -get hdfs:///user/braemy/alternative_titles2.json/ /dlabdata1/braemy/wikidataNER/alternative_titles2

#python3 convert_to_dict.py