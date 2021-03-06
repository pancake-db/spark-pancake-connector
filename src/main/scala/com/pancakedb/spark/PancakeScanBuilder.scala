package com.pancakedb.spark

import com.google.protobuf.{Timestamp => PbTimestamp}
import com.pancakedb.client.PancakeClient
import com.pancakedb.idl
import com.pancakedb.idl.PartitionFieldComparison.Operator
import com.pancakedb.idl._
import com.pancakedb.spark.Exceptions.UnrecognizedPartitionDataTypeException
import com.pancakedb.spark.PancakeScan.PancakeInputSegment
import com.pancakedb.spark.PancakeScanBuilder.partitionFieldValue
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import java.sql
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class PancakeScanBuilder(
  params: Parameters,
  pancakeSchema: idl.Schema,
  private var requiredSchema: StructType,
) extends ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
  private val partitionFilters: ArrayBuffer[Filter] = ArrayBuffer.empty
  private val pancakeFilters: ArrayBuffer[PartitionFilter] = ArrayBuffer.empty
  private val logger = LoggerFactory.getLogger(getClass)

  def loadSegments(): Array[InputPartition] = {
    val requiredColumns: Array[String] = requiredSchema.fields.map(_.name)
    val partitionColumns = pancakeSchema.getPartitioningMap.asScala
    val onlyPartitionColumns = requiredColumns.forall(partitionColumns.contains)

    val listSegmentsReq = ListSegmentsRequest.newBuilder()
      .setTableName(params.tableName)
      .addAllPartitionFilter(pancakeFilters.asJava)
      .setIncludeMetadata(onlyPartitionColumns)
      .build()

    val client = PancakeClientCache.getFromParams(params)
    val listSegmentsResp = client.grpc.listSegments(listSegmentsReq).get()
    val segments = listSegmentsResp
      .getSegmentsList
      .asScala
      .map(segment => PancakeInputSegment(segment, onlyPartitionColumns))
      .toArray[InputPartition]

    logger.info(s"Listed ${segments.length} segments for table scan")
    segments
  }

  override def build(): Scan = {
    PancakeScan(params, pancakeSchema, requiredSchema, loadSegments())
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val partitionColumns = pancakeSchema.getPartitioningMap.asScala

    def addFilter(attribute: String, value: Any, filter: Filter, operator: PartitionFieldComparison.Operator):  Unit = {
      partitionColumns.get(attribute).foreach(partitionMeta => {
        val comparison = PartitionFieldComparison.newBuilder()
          .setName(attribute)
          .setOperator(operator)
          .setValue(partitionFieldValue(partitionMeta.getDtype, value))

        partitionFilters += filter
        val pancakeFilter = PartitionFilter.newBuilder()
          .setComparison(comparison)
          .build()
        pancakeFilters += pancakeFilter
      })
    }

    filters.foreach({
      case EqualTo(attribute, value) =>
        addFilter(attribute, value, EqualTo(attribute, value), Operator.EQ_TO)
      case LessThan(attribute, value) =>
        addFilter(attribute, value, LessThan(attribute, value), Operator.LESS)
      case LessThanOrEqual(attribute, value) =>
        addFilter(attribute, value, LessThanOrEqual(attribute, value), Operator.LESS_OR_EQ_TO)
      case GreaterThan(attribute, value) =>
        addFilter(attribute, value, GreaterThan(attribute, value), Operator.GREATER)
      case GreaterThanOrEqual(attribute, value) =>
        addFilter(attribute, value, GreaterThanOrEqual(attribute, value), Operator.GREATER_OR_EQ_TO)
      case _ =>
    })
    //could further optimize by returning only non-pushed filters
    filters
  }

  override def pushedFilters(): Array[Filter] = {
    partitionFilters.toArray
  }
}

object PancakeScanBuilder {
  def partitionFieldValue(dtype: PartitionDataType, value: Any): PartitionFieldValue = {
    val builder = PartitionFieldValue.newBuilder()
    dtype match {
      case PartitionDataType.INT64 =>
        builder.setInt64Val(value.asInstanceOf[Long])
      case PartitionDataType.STRING =>
        builder.setStringVal(value.asInstanceOf[String])
      case PartitionDataType.BOOL =>
        builder.setBoolVal(value.asInstanceOf[Boolean])
      case PartitionDataType.TIMESTAMP_MINUTE =>
        val sqlTimestamp = value.asInstanceOf[sql.Timestamp]
        val pbTimestamp = PbTimestamp
          .newBuilder()
          .setSeconds(Math.floorDiv(sqlTimestamp.getTime, 1000))
          .setNanos(sqlTimestamp.getNanos)
          .build()
        builder.setTimestampVal(pbTimestamp)
      case PartitionDataType.UNRECOGNIZED => throw UnrecognizedPartitionDataTypeException
    }
    builder.build()
  }
}
