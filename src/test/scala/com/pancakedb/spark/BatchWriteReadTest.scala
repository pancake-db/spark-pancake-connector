package com.pancakedb.spark

import com.pancakedb.client.PancakeClient
import com.pancakedb.idl._
import com.pancakedb.spark.BatchWriteReadTest.{TestRow, baseRow}
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.apache.spark.sql.SaveMode

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer

class BatchWriteReadTest extends SparkTestBase {
  import session.implicits._
  val host = "localhost"
  val port = 3842
  val tableName = "batch_write_read_test"

  // running this test requires a local instance of PancakeDB on the port
  "writing and reading back a df" should "preserve row count" ignore {
    val client = PancakeClient(host, port)
    try {
      client.grpc.dropTable(DropTableRequest.newBuilder().setTableName(tableName).build()).get()
    } catch {
      case e: StatusRuntimeException if e.getStatus.getCode == Code.NOT_FOUND =>
    }
    val schema = Schema.newBuilder()
      .putPartitioning("part", PartitionMeta.newBuilder().setDtype(PartitionDataType.TIMESTAMP_MINUTE).build())
      .putColumns("i", ColumnMeta.newBuilder().setDtype(DataType.INT64).build())
      .putColumns("s", ColumnMeta.newBuilder().setDtype(DataType.STRING).build())
      .putColumns("l", ColumnMeta.newBuilder().setDtype(DataType.STRING).setNestedListDepth(1).build())
      .putColumns("b", ColumnMeta.newBuilder().setDtype(DataType.BYTES).build())
      .putColumns("t", ColumnMeta.newBuilder().setDtype(DataType.TIMESTAMP_MICROS).build())
      .putColumns("f", ColumnMeta.newBuilder().setDtype(DataType.FLOAT32).build())
      .putColumns("d", ColumnMeta.newBuilder().setDtype(DataType.FLOAT64).build())
      .build()
    val createTableReq = CreateTableRequest.newBuilder()
      .setTableName(tableName)
      .setSchema(schema)
      .setMode(CreateTableRequest.SchemaMode.OK_IF_EXACT)
      .build()
    client.grpc.createTable(createTableReq).get()

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
        b = Some(Array(3, 4, 5).map(_.toByte)),
        l = Some(Array("l0", "l1")),
        t = Some(ts),
        f = Some(1.11.toFloat),
        d = Some(2.22),
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
    val rows = df.select("part", "i", "s", "b", "l", "t", "f", "d").where(df.col("part") === partitionTs).collect()
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
    b: Option[Array[Byte]],
    t: Option[Timestamp],
    f: Option[Float],
    d: Option[Double],
  )

  val baseRow: TestRow = TestRow(new Timestamp(0), None, None, None, None, None, None, None)
}
