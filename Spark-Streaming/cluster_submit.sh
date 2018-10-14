spark-submit --master yarn-client --driver-memory 2g --num-executors 3 --executor-memory 3g --conf spark.executor.cores=5 realtime-anomaly-detection-1.0-SNAPSHOT-jar-with-dependencies.jar
