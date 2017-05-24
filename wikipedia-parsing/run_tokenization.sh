#!/usr/bin/env bash

#remove the output in hdfs in case it already exists
hadoop fs -rm -r hdfs:///user/braemy/wikipedia_dataset_$1$2.json
#run the tokenization
if [ "$#" -e 2 ];
then
    spark-submit --master yarn --num-executors 150 --executor-memory 4G --conf spark.yarn.executor.memoryOverhead=2048 tokenize_wikipedia.py --id_max $1 --subpart $2
else
    spark-submit --master yarn --num-executors 150 --executor-memory 4G --conf spark.yarn.executor.memoryOverhead=2048 tokenize_wikipedia.py --id_max $1
fi
#remove the output file in the file system
rm -rf /dlabdata1/braemy/wikidataNER/wikipedia_dataset_$1/
#get the data from hdfs to file sysmte
hadoop fs -get hdfs:///user/braemy/wikipedia_dataset_$1$2.json/ /dlabdata1/braemy/wikidataNER/wikipedia_dataset_$1$2
# convert the data to conll format
python3 convert_to_ner_file.py --id $1 --subpart $2