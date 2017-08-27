#!/usr/bin/env bash


#run the tokenization
if [ "$#" -eq 4 ];
then
    echo "$4"
    #remove the output in hdfs in case it already exists
    hadoop fs -rm -r hdfs:///user/braemy/wikipedia_dataset_$2_$4.json
    spark-submit --master yarn --num-executors $1 --executor-memory 8G --conf spark.yarn.executor.memoryOverhead=4096 tokenize_wikipedia.py --filter $2 --method $3 --subpart $4
else
    #remove the output in hdfs in case it already exists
    hadoop fs -rm -r hdfs:///user/braemy/wikipedia_dataset_$2.json
    spark-submit --master yarn --num-executors $1 --executor-memory 8G --conf spark.yarn.executor.memoryOverhead=4096 tokenize_wikipedia.py --filter $2 --method $3
fi

if [ "$#" -eq 4 ];
then
    #remove the output file in the file system
    rm -rf /dlabdata1/braemy/wikidataNER/$3/wikipedia_dataset_$2_$4/
    #get the data from hdfs to file sysmte
    hadoop fs -get hdfs:///user/braemy/wikipedia_dataset_$2_$4.json/ /dlabdata1/braemy/wikidataNER/$3/wikipedia_dataset_$2_$4
    # convert the data to conll format
    python3 ../convert_to_ner_file.py --id $2 --method $3 --subpart $4
else
    #remove the output file in the file system
    rm -rf /dlabdata1/braemy/wikidataNER/$3/wikipedia_dataset_$2/
    #get the data from hdfs to file sysmte
    hadoop fs -get hdfs:///user/braemy/wikipedia_dataset_$2.json/ /dlabdata1/braemy/wikidataNER/$3/wikipedia_dataset_$2
    # convert the data to conll format
    python3 ../convert_to_ner_file.py --id $2 --method $3
fi

