package com.pancakedb.spark

import com.google.protobuf.{Timestamp => PbTimestamp}
import com.pancakedb.client.{PancakeClient, RepLevelsColumn}
import com.pancakedb.idl._
import com.pancakedb.spark.AtomHandlers.{BooleanHandler, ByteHandler, DoubleHandler, LongHandler}
import com.pancakedb.spark.Exceptions.{UnrecognizedDataTypeException, UnrecognizedPartitionDataTypeException}
import com.pancakedb.spark.PancakeScan.PancakeInputSegment
import com.pancakedb.spark.PancakeSegmentReader.fillPartitionColumn
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class PancakeSegmentReader(
  params: Parameters,
  pancakeSchema: Schema,
  requiredSchema: StructType,
  client: PancakeClient,
  inputSegment: PancakeInputSegment,
) extends PartitionReader[ColumnarBatch] {
  private val logger = LoggerFactory.getLogger(getClass)

  override def next(): Boolean = {
    // there's always just 1 batch
    false
  }

  override def get(): ColumnarBatch = {
    val segment = inputSegment.segment
    val partitionFields = segment.getPartitionList.asScala
      .map(partitionField => partitionField.getName -> partitionField)
      .toMap
    val columnMetas = pancakeSchema.getColumnsList.asScala
      .map(meta => meta.getName -> meta)
      .toMap
    val futures = requiredSchema
      .fields
      .map(_.name)
      .filter(!partitionFields.contains(_))
      .map(columnName => columnName -> Future {
        val startTime = System.currentTimeMillis()
        val res = client.decodeSegmentRepLevelsColumn(
          params.tableName,
          segment.getPartitionList.asScala.to[ArrayBuffer],
          segment.getSegmentId,
          columnMetas(columnName),
        )
        logger.debug(
          s"""Queried and decoded columnar bytes for segment
             |${segment.getSegmentId}
             |column $columnName in
             |${System.currentTimeMillis() - startTime}ms""".stripMargin
        )
        res
      })
      .toMap[String, Future[RepLevelsColumn[_]]]
    val ordinaryColumns = futures.mapValues(Await.result(_, Duration.Inf))

    val n = if (inputSegment.useSegmentCounts) {
      segment.getMetadata.getCount
    } else {
      ordinaryColumns.values.map(_.nRows).reduce((a, b) => a.min(b))
    }

    val columnVectors = OnHeapColumnVector.allocateColumns(n, requiredSchema)
    val batch = new ColumnarBatch(columnVectors.map(_.asInstanceOf[ColumnVector]), n)
    requiredSchema
      .map(_.name)
      .zipWithIndex
      .foreach({ case (colName, i) =>
        if (partitionFields.contains(colName)) {
          fillPartitionColumn(columnVectors(i), n, partitionFields(colName))
        } else {
          fillOrdinaryColumn(columnVectors(i), ordinaryColumns(colName), columnMetas(colName))
        }
      })

    batch
  }

  override def close(): Unit = {}

  def rowReader(): PartitionReader[InternalRow] = {
    val base = this
    new PartitionReader[InternalRow] {
      val batch: ColumnarBatch = base.get()
      var i: Int = -1

      override def next(): Boolean = {
        i += 1
        i < batch.numRows()
      }

      override def get(): InternalRow = {
        batch.getRow(i)
      }

      override def close(): Unit = {}
    }
  }

  def fillOrdinaryColumn(
    vector: WritableColumnVector,
    repLevelsColumn: RepLevelsColumn[_],
    meta: ColumnMeta,
  ): Unit = {
    val startTime = System.currentTimeMillis()
    meta.getDtype match {
      case DataType.INT64 | DataType.TIMESTAMP_MICROS =>
        LongHandler.fillVector(repLevelsColumn, vector)
      case DataType.BOOL =>
        BooleanHandler.fillVector(repLevelsColumn, vector)
      case DataType.FLOAT64 =>
        DoubleHandler.fillVector(repLevelsColumn, vector)
      case DataType.STRING | DataType.BYTES =>
        ByteHandler.fillVector(repLevelsColumn, vector)
      case DataType.UNRECOGNIZED =>
        throw UnrecognizedDataTypeException
    }
    logger.debug(
      s"""Filled Spark vector for segment ${inputSegment.segment.getSegmentId}
         |column ${meta.getName} in
         |${System.currentTimeMillis() - startTime}ms""".stripMargin
    )
  }
}

object PancakeSegmentReader {
  // Spark's Timestamp type is just stored as epoch micros long
  // so we lose the full time range and ns precision
  def timestampToMicros(pbTimestamp: PbTimestamp): Long = {
    pbTimestamp.getSeconds * 1000000 + pbTimestamp.getNanos / 1000
  }

  def fillPartitionColumn(column: WritableColumnVector, n: Int, value: PartitionField): Unit = {
    value.getValueCase match {
      case PartitionField.ValueCase.INT64_VAL => column.putLongs(0, n, value.getInt64Val)
      case PartitionField.ValueCase.STRING_VAL =>
        val bytes = value.getStringVal.getBytes
        for (i <- 0 until n) {
          column.putByteArray(i, bytes)
        }
      case PartitionField.ValueCase.BOOL_VAL => column.putBooleans(0, n, value.getBoolVal)
      case PartitionField.ValueCase.TIMESTAMP_VAL =>
        val pbTimestamp = value.getTimestampVal
        column.putLongs(0, n, timestampToMicros(pbTimestamp))
      case PartitionField.ValueCase.VALUE_NOT_SET => throw UnrecognizedPartitionDataTypeException
    }
    column.setIsConstant()
  }


  def generateRawValueExtractor(col: ColumnMeta, vector: WritableColumnVector, traverseLevel: Int): FieldValue => _ = {
    if (traverseLevel == col.getNestedListDepth) {
      col.getDtype match {
        case DataType.INT64 => (fv: FieldValue) => vector.appendLong(fv.getInt64Val)
        case DataType.STRING => (fv: FieldValue) => {
          val bytes = fv.getStringValBytes.toByteArray
          vector.appendByteArray(bytes, 0, bytes.length)
        }
        case DataType.BOOL => (fv: FieldValue) => vector.appendBoolean(fv.getBoolVal)
        case DataType.BYTES => (fv: FieldValue) => {
          val bytes = fv.getBytesVal.toByteArray
          vector.appendByteArray(bytes, 0, bytes.length)
        }
        case DataType.FLOAT64 => (fv: FieldValue) => vector.appendDouble(fv.getFloat64Val)
        case DataType.TIMESTAMP_MICROS => (fv: FieldValue) =>
          val pbTimestamp = fv.getTimestampVal
          vector.appendLong(timestampToMicros(pbTimestamp))
        case DataType.UNRECOGNIZED => throw UnrecognizedDataTypeException
      }
    } else {
      val subroutine = generateRawValueExtractor(col, vector.getChild(0), traverseLevel + 1)
      (fv: FieldValue) => {
        val subFvs = fv.getListVal.getValsList.asScala
        subFvs.foreach(subFv => subroutine(subFv))
        vector.appendArray(subFvs.length)
      }
    }
  }

  def generateValueFiller(col: ColumnMeta, vector: WritableColumnVector): FieldValue => Unit = {
    var i = 0
    val rawExtractor = generateRawValueExtractor(col, vector, 0)
    (fv: FieldValue) => {
      if (fv.getValueCase.getNumber > 0) {
        rawExtractor(fv)
      } else {
        vector.putNull(i)
      }
      i += 1
    }
  }
}