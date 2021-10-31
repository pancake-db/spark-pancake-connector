# How to Run an Example

`cd`'d into this directory,
```
sbt assembly
sh speed_test.sh --host $HOST_IP --port $PORT
```

This will run the `spark-submit` command you see in `speed_test.sh`.
You can also copy a table from S3 into PancakeDB with the `copy.sh` example like so:
```
sh copy.sh --host $HOST_IP --port $PORT --table_name $TABLE_NAME --bucket $S3_BUCKET --tables_dir $S3_FOLDER
```

Note that fast speeds will only be achievable if you are running your Spark instance
in the same datacenter (or AWS region) as your PancakeDB instance.

See the corresponding scala code for
[speed test](https://github.com/pancake-db/spark-pancake-connector/blob/main/examples/src/main/scala/com/pancakedb/spark/SpeedTestPipeline.scala)
and
[copy](https://github.com/pancake-db/spark-pancake-connector/blob/main/examples/src/main/scala/com/pancakedb/spark/CopyToPancakePipeline.scala).