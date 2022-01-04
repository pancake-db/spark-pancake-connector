package com.pancakedb.spark

import com.pancakedb.client.PancakeClient
import com.pancakedb.idl
import com.pancakedb.idl.{ListSegmentsRequest, PartitionFilter, Segment}
import com.pancakedb.spark.PancakeScan.PancakeInputSegment
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class PancakeScan(
  params: Parameters,
  pancakeSchema: idl.Schema,
  requiredSchema: StructType,
  filters: ArrayBuffer[PartitionFilter],
  client: PancakeClient
) extends Scan with Batch with PartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)
  val requiredColumns: Array[String] = requiredSchema.fields.map(_.name)

  // TODO we obviously do support columnar reads, but what is this option
  // override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def planInputPartitions(): Array[InputPartition] = {
    val partitionColumns = pancakeSchema.getPartitioningMap.asScala
    val useSegmentMeta = requiredColumns.forall(partitionColumns.contains)

    val listSegmentsReq = ListSegmentsRequest.newBuilder()
      .setTableName(params.tableName)
      .addAllPartitionFilter(filters.asJava)
      .setIncludeMetadata(useSegmentMeta)
      .build()

    val listSegmentsResp = client.Api.listSegments(listSegmentsReq)
    val segments = listSegmentsResp
      .getSegmentsList
      .asScala
      .map(segment => PancakeInputSegment(segment, useSegmentMeta))
      .toArray[InputPartition]

    logger.info(s"Listed ${segments.length} segments for table scan")
    segments
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    this
  }

  override def readSchema(): StructType = {
    requiredSchema
  }

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    createColumnarReader(partition)
      .rowReader()
  }

  override def createColumnarReader(partition: InputPartition): PancakeSegmentReader = {
    assert(partition.isInstanceOf[PancakeInputSegment])
    PancakeSegmentReader(
      params,
      pancakeSchema,
      requiredSchema,
      client,
      partition.asInstanceOf[PancakeInputSegment],
    )
  }

  override def toBatch: Batch = this
}

object PancakeScan {
  case class PancakeInputSegment(segment: Segment, useSegmentCounts: Boolean) extends InputPartition
}
