package com.pancakedb.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object CopyToPancakePipeline extends Pipeline {
  override def run(session: SparkSession, arguments: Arguments): Unit = {
    val host = arguments.string("host").get
    val port = arguments.int("port").getOrElse(3842)
    val bucket = arguments.string("bucket").get
    val tablesDir = arguments.string("tables_dir").get
    val tableName = arguments.string("table_name").get
    val maybeColumns = arguments.string("columns").map(_.split(","))

    var inputDf = session
      .read
      .parquet(s"s3a://$bucket/$tablesDir/$tableName")
    maybeColumns.foreach(columns => {
      inputDf = inputDf.select(columns.head, columns.tail: _*)
    })
    inputDf
      .write
      .format("pancake")
      .option("host", host)
      .option("port", port)
      .option("table_name", tableName)
      .mode(SaveMode.Overwrite)
      .save()
  }
}