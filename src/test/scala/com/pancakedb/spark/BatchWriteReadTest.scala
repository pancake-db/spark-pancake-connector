package com.pancakedb.spark

import com.pancakedb.client.Exceptions.HttpException
import com.pancakedb.client.PancakeClient
import com.pancakedb.idl._
import com.pancakedb.spark.BatchWriteReadTest.{TestRow, baseRow}
import org.apache.spark.sql.SaveMode

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer

class BatchWriteReadTest extends SparkTestBase {
  import session.implicits._
  val host = "localhost"
  val port = 1337

  // running this test requires a local instance of PancakeDB on the port
  "writing and reading back a df" should "preserve row count" ignore {
    val tableName = "batch_write_read_test"

    val client = PancakeClient(host, port)
    try {
      client.Api.dropTable(DropTableRequest.newBuilder().setTableName(tableName).build())
    } catch {
      case HttpException(404, _) =>
    }
    val schema = Schema.newBuilder()
      .addPartitioning(PartitionMeta.newBuilder().setName("part").setDtype(PartitionDataType.TIMESTAMP_MINUTE))
      .addColumns(ColumnMeta.newBuilder().setName("i").setDtype(DataType.INT64))
      .addColumns(ColumnMeta.newBuilder().setName("s").setDtype(DataType.STRING))
      .addColumns(ColumnMeta.newBuilder().setName("l").setDtype(DataType.STRING).setNestedListDepth(1))
      .addColumns(ColumnMeta.newBuilder().setName("by").setDtype(DataType.BYTES))
      .addColumns(ColumnMeta.newBuilder().setName("t").setDtype(DataType.TIMESTAMP_MICROS))
      .build()
    val createTableReq = CreateTableRequest.newBuilder()
      .setTableName(tableName)
      .setSchema(schema)
      .setMode(CreateTableRequest.SchemaMode.OK_IF_EXACT)
      .build()
    client.Api.createTable(createTableReq)

    val inputRows = ArrayBuffer.empty[TestRow]
    val partitionTs = new Timestamp(1632097320000L)
    val ts = new Timestamp(1632097326123L)
    for (_ <- 0 until 500) {
      inputRows += baseRow.copy(
        part=partitionTs,
      )
      inputRows += baseRow.copy(
        part = partitionTs,
        i = Some(33L),
        s = Some("ssss"),
        by = Some(Array(3, 4, 5).map(_.toByte)),
        l = Some(Array("l0", "l1")),
        t = Some(ts),
      )
    }
    val inputDf = session.createDataset[TestRow](inputRows)
    inputDf
      .write
      .format("pancake")
      .mode(SaveMode.Append)
      .option("host", host)
      .option("port", port)
      .option("table_name", tableName)
      .option("write_batch_size", 1) // test for race conditions in PancakeDB as much as possible
      .save()

    val df = session
      .read
      .format("pancake")
      .option("host", host)
      .option("port", port)
      .option("table_name", tableName)
      .load()
    val rows = df.select("part", "i", "s", "by", "l", "t").where(df.col("part") === partitionTs).collect()
    println(s"I found ${rows.length} rows:")
    for (i <- 0 until 20) {
      println(s"\trow $i: ${rows(i)}")
    }
    assertResult(inputRows.length)(rows.length)
  }

}

object BatchWriteReadTest {
  case class TestRow(
    part: Timestamp,
    i: Option[Long],
    s: Option[String],
    l: Option[Array[String]],
    by: Option[Array[Byte]],
    t: Option[Timestamp]
  )

  val baseRow: TestRow = TestRow(new Timestamp(0), None, None, None, None, None)
}
