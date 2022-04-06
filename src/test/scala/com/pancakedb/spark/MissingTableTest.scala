package com.pancakedb.spark

import com.pancakedb.spark.Exceptions.TableDoesNotExist

class MissingTableTest extends SparkTestBase {
  "reading a table" should "fail nicely" ignore {
    assertThrows[TableDoesNotExist] {
      val df = session.read
        .format("pancake")
        .option("host", "localhost")
        .option("port", 3842)
        .option("table_name", "non_existent_table")
        .load()
    }
  }
}
