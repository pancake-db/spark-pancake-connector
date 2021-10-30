package com.pancakedb.spark

import org.apache.spark.sql.SparkSession

object Main {
  val pipelines: Map[String, Pipeline] = Map(
    "SpeedTest" -> SpeedTestPipeline,
    "CopyToPancake" -> CopyToPancakePipeline,
  )
  def main(args: Array[String]): Unit = {
    val arguments = Arguments.parse(args)
    val session = SparkSession.builder().getOrCreate()

    pipelines(arguments.string("pipeline").get).run(session, arguments)
  }
}
