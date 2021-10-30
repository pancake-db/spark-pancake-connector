package com.pancakedb.spark

import org.apache.spark.sql.SparkSession

trait Pipeline {
  def run(session: SparkSession, arguments: Arguments): Unit
}
