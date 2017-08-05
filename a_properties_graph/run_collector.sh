#!/usr/bin/env bash

hadoop fs -rm -r hdfs:///user/braemy/wikidata.json
spark-submit --master yarn --num-executors 150 \
 --executor-memory 8G \
 --conf spark.driver.memory=32G \
 --packages com.databricks:spark-xml_2.10:0.4.1\
 --conf spark.yarn.executor.memoryOverhead=4096 properties_collector.py


rm -rf /dlabdata1/braemy/wikidataNER/wikidata
python build_mapping.py

