package com.pancakedb.spark

import com.pancakedb.spark.StreamingWriteTest.TestStreamRow
import org.apache.spark.sql.streaming.OutputMode

class StreamingWriteTest extends SparkTestBase {
  import session.implicits._
  val host = "localhost"
  val pancakePort = 1337
  val socketPort = 12345

  // running this test requires a local instance of PancakeDB on the port
  // AND a process writing to the socket port
  "streaming write" should "work" ignore {
    val tableName = "stream_test"

    val stream = session
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", socketPort)
      .load()
      .as[String]
      .map(line => {
        val b = if (Math.random() > 0.5) true else false
        TestStreamRow(
          line,
          b,
        )
      })
      .writeStream
      .format("pancake")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "/tmp/spark-pancake-test")
      .option("host", host)
      .option("port", pancakePort)
      .option("table_name", tableName)
      .start()

    Thread.sleep(20000)
    stream.stop()
  }
}

object StreamingWriteTest {
  case class TestStreamRow(
    line: String,
    b: Boolean,
  )
}
