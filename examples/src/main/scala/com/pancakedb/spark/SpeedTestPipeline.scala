package com.pancakedb.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SpeedTestPipeline extends Pipeline {
  override def run(session: SparkSession, arguments: Arguments): Unit = {
    val source = arguments.string("source").get.toLowerCase
    val tableName = arguments.string("table_name").getOrElse("1m_x1")
    val task = arguments.string("task").getOrElse("collect")
    val hang = arguments.boolean("hang").getOrElse(false) // to look at Spark UI before it closes
    val columnsArg = arguments.string("columns").get
    val columns = columnsArg.split(",")

    val startT = System.currentTimeMillis()

    val df = if (source == "pancake") {
      session
          .read
          .format("pancake")
          .option("host", arguments.string("host").get)
          .option("port", arguments.int("port").get)
          .option("table_name", tableName)
          .load()
    } else if (source == "s3/parquet") {
      val bucket = arguments.string("bucket").get
      val tablesDir = arguments.string("tables_dir").get
      session
          .read
          .parquet(s"s3a://$bucket/$tablesDir/$tableName")
    } else {
      throw new Exception(s"unknown source $source")
    }

    if (task == "collect") {
      val rows = df.select(columns.head, columns.tail: _*).collect()
      println(s"COLLECTED ${rows.length} ROWS FROM $tableName")
    } else if (task == "sum") {
      val aggs = columns.map(sum)
      val total = df.agg(aggs.head, aggs.tail: _*)
      println(s"SUMMED COLUMNS $columnsArg FROM $tableName: ${total.collect()(0)}")
    }
    val endT = System.currentTimeMillis()
    println(s"FINISHED TASK IN ${endT - startT}ms (includes time to start and stop Spark, typically several seconds)")

    if (hang) {
      Thread.sleep(1000000)
    }
  }
}
