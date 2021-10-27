package com.pancakedb.spark

import org.apache.spark.sql.SparkSession

class SparkTestBase extends TestBase {
  lazy val session: SparkSession = {
    SparkSession.builder()
        .master("local[2]")
        .getOrCreate()
  }
}
