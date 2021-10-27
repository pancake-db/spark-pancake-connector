spark-submit \
  --conf "spark.executor.extraJavaOptions=-Djava.library.path=../src/main/rust/target/release" \
  --conf "spark.driver.extraJavaOptions=-Djava.library.path=../src/main/rust/target/release" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --master "local[2]" \
  target/scala-2.12/spark-pancake-db-connector-examples-assembly-0.0.0.jar \
  --host 172.31.27.88 \
  --pipeline SpeedTest \
  --source "pancake" \
  --table_name bloated_10m_x1 \
  --task sum \
  --columns int64
