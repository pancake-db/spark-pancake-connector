package com.pancakedb.spark

import com.pancakedb.client.PancakeClient
import com.pancakedb.idl
import com.pancakedb.idl.{Field, PartitionField, WriteToPartitionRequest}
import com.pancakedb.spark.PancakeDataWriter.{HashedPartition, PancakeCommitMessage}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class PancakeDataWriter(
  params: Parameters,
  client: PancakeClient,
  numPartitions: Int,
  partitionId: Int,
  taskId: Long,
  partitionFieldGetters: Array[InternalRow => PartitionField],
  fieldGetters: Array[InternalRow => Field],
) extends DataWriter[InternalRow] {
  private val stagedRows = mutable.Map.empty[HashedPartition, ArrayBuffer[idl.Row]]
  private val logger = LoggerFactory.getLogger(getClass)

  def makeHashedPartition(row: InternalRow): HashedPartition = {
    val partition = partitionFieldGetters.map(getter => getter(row))
    HashedPartition.fromPartition(partition)
  }

  def makePancakeRow(row: InternalRow): idl.Row = {
    val values = fieldGetters.map(getter => getter(row))
    idl.Row.newBuilder()
      .addAllFields(values.toBuffer.asJava)
      .build()
  }

  private def flushPartition(partition: HashedPartition): Unit = {
    logger.debug(s"Flushing write of ${stagedRows(partition).length} rows to PancakeDB")
    val req = WriteToPartitionRequest.newBuilder()
      .setTableName(params.tableName)
      .addAllPartition(partition.partition.toBuffer.asJava)
      .addAllRows(stagedRows(partition).asJava)
      .build()
    stagedRows(partition) = ArrayBuffer.empty
    client.Api.writeToPartition(req)
  }

  override def write(record: InternalRow): Unit = {
    val partition = makeHashedPartition(record)
    val pancakeRow = makePancakeRow(record)
    if (stagedRows.contains(partition)) {
      stagedRows(partition) += pancakeRow
    } else {
      stagedRows(partition) = ArrayBuffer(pancakeRow)
    }

    if (stagedRows(partition).length == params.writeBatchSize) {
      flushPartition(partition)
    }
  }

  override def close(): Unit = {
    stagedRows.foreach({ case (partition, rows) =>
      if (rows.nonEmpty) {
        flushPartition(partition)
      }
    })
  }

  override def commit(): WriterCommitMessage = {
    PancakeCommitMessage()
  }

  override def abort(): Unit = {}
}

object PancakeDataWriter {
  case class PancakeCommitMessage() extends WriterCommitMessage

  case class HashedPartition(
    partition: Array[PartitionField],
    hash: Int,
  ) {
    override def hashCode(): Int = hash

    override def equals(obj: Any): Boolean = {
      val other = obj.asInstanceOf[HashedPartition]
      other.hash == hash && partition.sameElements(other.partition)
    }
  }

  object HashedPartition {
    def fromPartition(partition: Array[PartitionField]): HashedPartition = {
      // improvise a hash fn
      var hash = 0
      partition.foreach(field => {
        hash *= 1234577
        hash += field.hashCode()
      })
      HashedPartition(
        partition,
        hash,
      )
    }
  }
}
