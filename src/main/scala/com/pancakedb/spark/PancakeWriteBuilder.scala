package com.pancakedb.spark

import com.google.protobuf.{ByteString, Timestamp => PbTimestamp}
import com.pancakedb.client.PancakeClient
import com.pancakedb.idl._
import com.pancakedb.spark.Exceptions.{IncompatibleDataTypeException, UnrecognizedDataTypeException, UnrecognizedPartitionDataTypeException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DoubleType, LongType, StringType, StructType, TimestampType, DataType => SparkDataType}
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

case class PancakeWriteBuilder(
  params: Parameters,
  pancakeSchemaCache: SchemaCache,
  schema: StructType,
  client: PancakeClient,
) extends WriteBuilder with BatchWrite with StreamingWrite with SupportsTruncate {
  private val logger = LoggerFactory.getLogger(getClass)
  private var isTruncate = false

  // SupportsTruncate methods
  override def truncate(): WriteBuilder = {
    logger.info("TRUNCATE SET TO TRUE")
    isTruncate = true
    this
  }

  // WriteBuilder methods
  override def buildForBatch(): BatchWrite = {
    this
  }

  override def buildForStreaming(): StreamingWrite = {
    this
  }

  // BatchWrite methods
  override def createBatchWriterFactory(info: PhysicalWriteInfo): PancakeDataWriterFactory = {
    val dfIdxByName = schema.fields.indices.map(idx => schema.fields(idx).name -> idx).toMap

    val (exists, pancakeSchema) = pancakeSchemaCache.getOption match {
      case Some(pSchema) =>
        logger.info(s"Table ${params.tableName} is already known to exist; using its schema")
        (true, pSchema)
      case None =>
        val pSchema = PancakeWriteBuilder.defaultPancakeSchemaFor(schema)
        pancakeSchemaCache.set(pSchema)
        (false, pSchema)
    }

    if (exists && isTruncate) {
      logger.info(
        s"""Table ${params.tableName} exists but we are in truncate mode;
           | dropping it and recreating"""
          .stripMargin.replaceAll("\n", "")
      )
      val dropTableRequest = DropTableRequest.newBuilder()
        .setTableName(params.tableName)
        .build()
      client.Api.dropTable(dropTableRequest)
      val createTableRequest = CreateTableRequest.newBuilder()
        .setTableName(params.tableName)
        .setMode(CreateTableRequest.SchemaMode.FAIL_IF_EXISTS)
        .setSchema(pancakeSchema)
        .build()
      client.Api.createTable(createTableRequest)
    } else if (!exists) {
      logger.info(
        s"""Table ${params.tableName} does not exist yet;
           | creating it with unpartitioned schema for write"""
          .stripMargin.replaceAll("\n", "")
      )
      val createTableRequest = CreateTableRequest.newBuilder()
        .setTableName(params.tableName)
        .setMode(CreateTableRequest.SchemaMode.FAIL_IF_EXISTS)
        .setSchema(pancakeSchema)
        .build()
      client.Api.createTable(createTableRequest)
    }

    val partitionFieldGetters = pancakeSchema
      .getPartitioningList
      .asScala
      .map(meta => {
        val idx = dfIdxByName(meta.getName)
        val name = meta.getName
        val extractor = PancakeWriteBuilder.extractPartitionFieldFn(meta.getDtype, idx)
        row: InternalRow => {
          val builder = PartitionField.newBuilder()
            .setName(name)
          extractor(row, builder)
          builder.build()
        }
      })
      .toArray

    val fieldGetters = pancakeSchema
      .getColumnsList
      .asScala
      .map(meta => {
        val idx = dfIdxByName(meta.getName)
        val name = meta.getName
        val sparkDtype = schema.fields(idx).dataType
        val extractor = PancakeWriteBuilder.extractFieldValueFn(meta, sparkDtype, idx)
        row: InternalRow => {
          val valueBuilder = FieldValue.newBuilder()
          if (!row.isNullAt(idx)) {
            extractor(row, valueBuilder)
          }
          Field.newBuilder()
            .setName(name)
            .setValue(valueBuilder.build())
            .build()
        }
      })
      .toArray

    PancakeDataWriterFactory(
      params,
      client,
      info.numPartitions(),
      partitionFieldGetters,
      fieldGetters,
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}

  // StreamingWrite methods
  override def createStreamingWriterFactory(info: PhysicalWriteInfo): PancakeDataWriterFactory = {
    createBatchWriterFactory(info)
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

object PancakeWriteBuilder {
  def extractPartitionFieldFn(dataType: PartitionDataType, i: Int): (InternalRow, PartitionField.Builder) => Unit = {
    dataType match {
      case PartitionDataType.INT64 => (row: InternalRow, builder: PartitionField.Builder) => builder.setInt64Val(row.getLong(i))
      case PartitionDataType.STRING => (row: InternalRow, builder: PartitionField.Builder) => builder.setStringVal(row.getString(i))
      case PartitionDataType.BOOL => (row: InternalRow, builder: PartitionField.Builder) => builder.setBoolVal(row.getBoolean(i))
      case PartitionDataType.TIMESTAMP_MINUTE => (row: InternalRow, builder: PartitionField.Builder) => builder.setTimestampVal(microsToPbTimestamp(row.getLong(i)))
      case PartitionDataType.UNRECOGNIZED => throw UnrecognizedPartitionDataTypeException
    }
  }

  // Apparently Spark's Timestamp type is just stored as epoch micros
  // so we lose the full time range and ns precision
  def microsToPbTimestamp(ms: Long): PbTimestamp = {
    PbTimestamp.newBuilder()
      .setSeconds(Math.floorDiv(ms, 1000000))
      .setNanos(Math.floorMod(ms, 1000000) * 1000)
      .build()
  }

  def extractFieldValueFn(meta: ColumnMeta, sparkType: SparkDataType, i: Int): (InternalRow, FieldValue.Builder) => Unit = {
    if (meta.getNestedListDepth > 0) {
      (row: InternalRow, builder: FieldValue.Builder) => {
        val arr = row.getArray(i).toArray[Any](sparkType.asInstanceOf[ArrayType].elementType)
        nestedExtractFieldValueFn(meta)(arr, builder)
      }
    } else {
      val dataType = meta.getDtype
      dataType match {
        case DataType.INT64 => (row: InternalRow, builder: FieldValue.Builder) => builder.setInt64Val(row.getLong(i))
        case DataType.STRING => (row: InternalRow, builder: FieldValue.Builder) => builder.setStringVal(row.getString(i))
        case DataType.BOOL => (row: InternalRow, builder: FieldValue.Builder) => builder.setBoolVal(row.getBoolean(i))
        case DataType.BYTES => (row: InternalRow, builder: FieldValue.Builder) => builder.setBytesVal(ByteString.copyFrom(row.getBinary(i)))
        case DataType.FLOAT64 => (row: InternalRow, builder: FieldValue.Builder) => builder.setFloat64Val(row.getDouble(i))
        case DataType.TIMESTAMP_MICROS => (row: InternalRow, builder: FieldValue.Builder) => builder.setTimestampVal(microsToPbTimestamp(row.getLong(i)))
        case DataType.UNRECOGNIZED => throw UnrecognizedDataTypeException
      }
    }
  }

  def nestedExtractFieldValueFn(meta: ColumnMeta, depth: Int = 0): (Any, FieldValue.Builder) => Unit = {
    if (depth == meta.getNestedListDepth) {
      meta.getDtype match {
        case DataType.INT64 => (value: Any, builder: FieldValue.Builder) => builder.setInt64Val(value.asInstanceOf[Long])
        case DataType.STRING => (value: Any, builder: FieldValue.Builder) => builder.setStringVal(value.asInstanceOf[UTF8String].toString)
        case DataType.BOOL => (value: Any, builder: FieldValue.Builder) => builder.setBoolVal(value.asInstanceOf[Boolean])
        case DataType.BYTES => (value: Any, builder: FieldValue.Builder) => builder.setBytesVal(ByteString.copyFrom(value.asInstanceOf[Array[Byte]]))
        case DataType.FLOAT64 => (value: Any, builder: FieldValue.Builder) => builder.setFloat64Val(value.asInstanceOf[Double])
        case DataType.TIMESTAMP_MICROS => (value: Any, builder: FieldValue.Builder) => builder.setTimestampVal(microsToPbTimestamp(value.asInstanceOf[Long]))
        case DataType.UNRECOGNIZED => throw UnrecognizedDataTypeException
      }
    } else {
      val subFn = nestedExtractFieldValueFn(meta, depth + 1)
      (value, builder) => {
        val listVal = RepeatedFieldValue.newBuilder()
          .addAllVals(value.asInstanceOf[Array[Any]].map(subValue => {
            val builder = FieldValue.newBuilder()
            subFn(subValue, builder)
            builder.build()
          }).toBuffer.asJava)
        builder.setListVal(listVal)
      }
    }
  }

  def defaultPancakeSchemaFor(schema: StructType): Schema = {
    val columns = schema.fields
      .map(structField => {
        var depth = 0
        var pDtype = Option.empty[DataType]
        var sparkDtype = structField.dataType
        while (pDtype.isEmpty) {
          sparkDtype match {
            case ArrayType(subType, _) =>
              depth += 1
              sparkDtype = subType
            case BooleanType => pDtype = Some(DataType.BOOL)
            case LongType => pDtype = Some(DataType.INT64)
            case StringType => pDtype = Some(DataType.STRING)
            case DoubleType => pDtype = Some(DataType.FLOAT64)
            case BinaryType => pDtype = Some(DataType.BYTES)
            case TimestampType => pDtype = Some(DataType.TIMESTAMP_MICROS)
            case _ => throw IncompatibleDataTypeException(sparkDtype)
          }
        }

        ColumnMeta.newBuilder()
          .setName(structField.name)
          .setNestedListDepth(depth)
          .setDtype(pDtype.get)
          .build()
      })
    Schema.newBuilder()
      .addAllColumns(columns.toBuffer.asJava)
      .build()
  }
}
