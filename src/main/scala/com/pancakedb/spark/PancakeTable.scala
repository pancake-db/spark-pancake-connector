package com.pancakedb.spark

import com.pancakedb.client.PancakeClient
import com.pancakedb.idl
import com.pancakedb.idl._
import com.pancakedb.spark.Exceptions.{UnrecognizedDataTypeException, UnrecognizedPartitionDataTypeException}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class PancakeTable(
  @transient sqlContext: SQLContext,
  params: Parameters,
  providedSchema: Option[StructType],
) extends Table with SupportsRead with SupportsWrite {
  private val logger = LoggerFactory.getLogger(getClass)
  private val client: PancakeClient = {
    logger.info(s"Initializing Pancake client with host ${params.host} and port ${params.port}")
    PancakeClient(host = params.host, port = params.port)
  }
  private val pancakeSchemaCache = SchemaCache(params.tableName, client)

  override def name(): String = params.tableName

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE,
      TableCapability.TRUNCATE,
    ).asJava
  }

  // Pancake tables are often partitioned, but we'd have to construct
  // a `Transform`, and all the pre-existing implementations are private.
  // This is only used in batch writing (which we don't support) and
  // describe table, so we can skip it.
  override lazy val partitioning: Array[Transform] = Array.empty

  def schema: StructType = {
    if (providedSchema.isDefined) {
      logger.debug(s"Using provided schema for table ${name()}")
      providedSchema.get
    } else {
      logger.debug(s"Using queried schema for table ${name()}")
      PancakeTable.convertSchema(pancakeSchemaCache.get)
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val params = Parameters.fromCaseInsensitiveStringMap(options)
    PancakeScanBuilder(params, pancakeSchemaCache.get, schema, client)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    PancakeWriteBuilder(params, pancakeSchemaCache, info.schema(), client)
  }
}

object PancakeTable {
  def convertSchema(pancakeSchema: Schema): StructType = {
    val fields = ArrayBuffer.empty[StructField]
    pancakeSchema.getColumnsMap.forEach({case (columnName, columnMeta) =>
      fields += columnMetaToStructField(columnName, columnMeta)
    })
    pancakeSchema.getPartitioningMap.forEach({case (partitioningName, partitioningMeta) =>
      fields += partitioningMetaToStructField(partitioningName, partitioningMeta)
    })
    StructType(fields)
  }

  def convertDataType(dtype: idl.DataType, nestingDepth: Int): DataType = {
    var sparkDtype = dtype match {
      case idl.DataType.INT64 => DataTypes.LongType
      case idl.DataType.STRING => DataTypes.StringType
      case idl.DataType.BOOL => DataTypes.BooleanType
      case idl.DataType.BYTES => DataTypes.BinaryType
      case idl.DataType.FLOAT32 => DataTypes.FloatType
      case idl.DataType.FLOAT64 => DataTypes.DoubleType
      case idl.DataType.TIMESTAMP_MICROS => DataTypes.TimestampType
      case idl.DataType.UNRECOGNIZED => throw UnrecognizedDataTypeException
    }
    for (_ <- 0 until nestingDepth) {
      sparkDtype = DataTypes.createArrayType(sparkDtype)
    }
    sparkDtype
  }

  def columnMetaToStructField(name: String, col: ColumnMeta): StructField = {
    StructField.apply(name, convertDataType(col.getDtype, col.getNestedListDepth))
  }

  def partitioningMetaToStructField(name: String, meta: PartitionMeta): StructField = {
    val dtype = meta.getDtype match {
      case PartitionDataType.INT64 => DataTypes.LongType
      case PartitionDataType.STRING => DataTypes.StringType
      case PartitionDataType.BOOL => DataTypes.BooleanType
      case PartitionDataType.TIMESTAMP_MINUTE => DataTypes.TimestampType
      case PartitionDataType.UNRECOGNIZED => throw UnrecognizedPartitionDataTypeException
    }
    StructField.apply(name, dtype)
  }
}
