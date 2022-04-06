package com.pancakedb.spark

import com.pancakedb.client.PancakeClient
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}

case class PancakeDataWriterFactory(
  params: Parameters,
  numPartitions: Int,
  partitionFieldGetters: Array[PartitionFieldGetter],
  fieldGetters: Array[FieldGetter],
) extends DataWriterFactory with StreamingDataWriterFactory {
  // for batch
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    PancakeDataWriter(
      params,
      numPartitions,
      partitionId,
      taskId,
      partitionFieldGetters,
      fieldGetters,
    )
  }

  // for streaming
  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    createWriter(partitionId, taskId)
  }
}
