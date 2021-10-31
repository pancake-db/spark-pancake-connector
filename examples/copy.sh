spark-submit \
  --conf "spark.driver.memory=4g" \
  --conf "spark.executor.memory=4g" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --master "local[4]" \
  target/scala-2.12/spark-pancake-db-connector-examples-assembly-0.0.0.jar \
  --pipeline CopyToPancake \
  "$@"
