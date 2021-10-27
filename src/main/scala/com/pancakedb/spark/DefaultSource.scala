package com.pancakedb.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class DefaultSource
  extends DataSourceRegister
    with TableProvider {
  override def shortName(): String = "pancake"

  override def supportsExternalMetadata(): Boolean = true

  lazy val sparkSession: SparkSession = SparkSession.active

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    PancakeTable(
      sparkSession.sqlContext,
      Parameters.fromCaseInsensitiveStringMap(options),
      None,
    ).schema
  }

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table = {
    PancakeTable(
      sparkSession.sqlContext,
      Parameters.fromProperties(properties),
      Some(schema),
    )
  }

//  override def createRelation(
//    sqlContext: SQLContext,
//    mode: SaveMode,
//    parameters: Map[String, String],
//    data: DataFrame
//  ): BaseRelation = {
//    val params = Parameters.fromMap(parameters)
//    PancakeTable(
//      sqlContext,
//      params,
//
//    )
//  }
}
