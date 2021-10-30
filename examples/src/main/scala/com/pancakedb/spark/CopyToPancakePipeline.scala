package com.pancakedb.spark

import com.pancakedb.client.PancakeClient
import com.pancakedb.idl.{ColumnMeta, CreateTableRequest, DataType, Schema}
import org.apache.spark.sql.{SaveMode, SparkSession}

object CopyToPancakePipeline extends Pipeline {
  override def run(session: SparkSession, arguments: Arguments): Unit = {
    val host = arguments.string("host").get
    val port = arguments.int("port").getOrElse(1337)
    val bucket = arguments.string("bucket").get
    val tablesDir = arguments.string("tables_dir").get
    val tableName = arguments.string("table_name").getOrElse("1m_x1")
    val columns = arguments.string("columns").get.split(",")

    val client = PancakeClient(host, port)
    val schema = Schema.newBuilder()
      .addColumns(ColumnMeta.newBuilder().setName("int64").setDtype(DataType.INT64))
      .addColumns(ColumnMeta.newBuilder().setName("float64").setDtype(DataType.FLOAT64))
      .addColumns(ColumnMeta.newBuilder().setName("string").setDtype(DataType.STRING))
      .addColumns(ColumnMeta.newBuilder().setName("bool").setDtype(DataType.BOOL))
      .build()
    val createTableReq = CreateTableRequest.newBuilder()
      .setTableName(tableName)
      .setSchema(schema)
      .setMode(CreateTableRequest.SchemaMode.OK_IF_EXACT)
      .build()
    client.Api.createTable(createTableReq)
    session
      .read
      .parquet(s"s3a://$bucket/$tablesDir/$tableName")
      .select(columns.head, columns.tail: _*)
      .write
      .format("pancake")
      .option("host", host)
      .option("port", port)
      .option("table_name", tableName)
      .mode(SaveMode.Append)
      .save()
  }
}