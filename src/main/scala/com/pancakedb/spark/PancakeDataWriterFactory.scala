package com.pancakedb.spark

import com.pancakedb.client.PancakeClient
import com.pancakedb.idl._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}

case class PancakeDataWriterFactory(
  params: Parameters,
  client: PancakeClient,
  numPartitions: Int,
  partitionFieldGetters: Array[InternalRow => PartitionField],
  fieldGetters: Array[InternalRow => Field],
) extends DataWriterFactory with StreamingDataWriterFactory {
  // for batch
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    PancakeDataWriter(
      params,
      client,
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
