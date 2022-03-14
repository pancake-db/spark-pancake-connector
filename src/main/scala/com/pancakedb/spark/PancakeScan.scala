package com.pancakedb.spark

import com.pancakedb.client.PancakeClient
import com.pancakedb.idl
import com.pancakedb.idl.Segment
import com.pancakedb.spark.PancakeScan.PancakeInputSegment
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType

case class PancakeScan(
  params: Parameters,
  pancakeSchema: idl.Schema,
  requiredSchema: StructType,
  segments: Array[InputPartition],
  client: PancakeClient
) extends Scan with Batch with PartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def planInputPartitions(): Array[InputPartition] = {
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
  case class PancakeInputSegment(
    segment: Segment,
    onlyPartitionColumns: Boolean,
  ) extends InputPartition
}
