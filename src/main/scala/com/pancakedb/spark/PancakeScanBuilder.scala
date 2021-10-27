package com.pancakedb.spark

import com.google.protobuf.{Timestamp => PbTimestamp}
import com.pancakedb.client.PancakeClient
import com.pancakedb.idl
import com.pancakedb.idl.{PartitionDataType, PartitionField, PartitionFilter}
import com.pancakedb.spark.Exceptions.UnrecognizedPartitionDataTypeException
import com.pancakedb.spark.PancakeScanBuilder.setPartitionFieldValue
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import java.sql
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class PancakeScanBuilder(
  params: Parameters,
  pancakeSchema: idl.Schema,
  private var requiredSchema: StructType,
  client: PancakeClient,
) extends ScanBuilder with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
  private val partitionFilters: ArrayBuffer[Filter] = ArrayBuffer.empty
  private val pancakeFilters: ArrayBuffer[PartitionFilter] = ArrayBuffer.empty

  override def build(): Scan = {
    PancakeScan(params, pancakeSchema, requiredSchema, pancakeFilters, client)
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val partitionColumns = pancakeSchema.getPartitioningList.asScala
      .map(partitionMeta => partitionMeta.getName -> partitionMeta)
      .toMap

    def addFilter(attribute: String, value: Any, filter: Filter, makePancakeFilter: PartitionField => PartitionFilter):  Unit = {
      partitionColumns.get(attribute).foreach(partitionMeta => {
        val partitionFieldBuilder = PartitionField.newBuilder()
          .setName(attribute)
        setPartitionFieldValue(partitionFieldBuilder, partitionMeta.getDtype, value)

        partitionFilters += filter
        val pancakeFilter = makePancakeFilter(partitionFieldBuilder.build())
        pancakeFilters += pancakeFilter
      })
    }

    filters.foreach({
      case EqualTo(attribute, value) =>
        addFilter(attribute, value, EqualTo(attribute, value), field => PartitionFilter.newBuilder().setEqualTo(field).build())
      case LessThan(attribute, value) =>
        addFilter(attribute, value, LessThan(attribute, value), field => PartitionFilter.newBuilder().setLessThan(field).build())
      case LessThanOrEqual(attribute, value) =>
        addFilter(attribute, value, LessThanOrEqual(attribute, value), field => PartitionFilter.newBuilder().setLessOrEqTo(field).build())
      case GreaterThan(attribute, value) =>
        addFilter(attribute, value, GreaterThan(attribute, value), field => PartitionFilter.newBuilder().setGreaterThan(field).build())
      case GreaterThanOrEqual(attribute, value) =>
        addFilter(attribute, value, GreaterThanOrEqual(attribute, value), field => PartitionFilter.newBuilder().setGreaterOrEqTo(field).build())
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
  def setPartitionFieldValue(builder: PartitionField.Builder, dtype: PartitionDataType, value: Any): Unit = {
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
  }
}
