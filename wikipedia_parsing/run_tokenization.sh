#!/usr/bin/env bash

#remove the output in hdfs in case it already exists
hadoop fs -rm -r hdfs:///user/braemy/wikipedia_dataset_$2$4.json
#run the tokenization
if [ "$#" -eq 4 ];
then
    spark-submit --master yarn --num-executors $1 --executor-memory 4G --conf spark.yarn.executor.memoryOverhead=1024 tokenize_wikipedia.py --filter $2 --method $3 --subpart $4
else
    spark-submit --master yarn --num-executors $1 --executor-memory 4G --conf spark.yarn.executor.memoryOverhead=1024 tokenize_wikipedia.py --filter $2 --method $3
fi
#remove the output file in the file system
rm -rf /dlabdata1/braemy/wikidataNER/$3/wikipedia_dataset_$2$4/
#get the data from hdfs to file sysmte
hadoop fs -get hdfs:///user/braemy/wikipedia_dataset_$2$4.json/ /dlabdata1/braemy/wikidataNER/$3/wikipedia_dataset_$2$4
# convert the data to conll format
if [ "$#" -eq 4 ];
then
    python3 convert_to_ner_file.py --id $2 --method $3--subpart $4
else
    python3 convert_to_ner_file.py --id $2 --method $3
fi

