package com.pancakedb.spark

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.mutable

case class Parameters(
  tableName: String,
  host: String,
  port: Int,
  // WRITE SPECIFIC
  var errorIfExists: Boolean,
  writeBatchSize: Int,
)

object Parameters {
  val DefaultWriteBatchSize: Int = 256

  def fromMap(params: Map[String, String]): Parameters = {
    Parameters(
      tableName = params("table_name"),
      host = params("host"),
      port = params("port").toInt,
      errorIfExists = params.getOrElse("error_if_exists", "false").toBoolean,
      writeBatchSize = params.get("write_batch_size").map(_.toInt).getOrElse(DefaultWriteBatchSize)
    )
  }

  def fromCaseInsensitiveStringMap(params: CaseInsensitiveStringMap): Parameters = {
    val map = mutable.Map.empty[String, String]
    params.entrySet().forEach(entry => {
      map(entry.getKey) = entry.getValue
    })
    fromMap(map.toMap)
  }

  def fromProperties(properties: java.util.Map[String, String]): Parameters = {
    val map = mutable.Map.empty[String, String]
    properties.entrySet().forEach(entry => {
      map(entry.getKey) = entry.getValue
    })
    fromMap(map.toMap)
  }
}
