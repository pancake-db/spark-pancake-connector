package com.pancakedb.spark

import com.pancakedb.client.PancakeClient
import com.pancakedb.idl
import com.pancakedb.idl.{FieldValue, PartitionFieldValue, WriteToPartitionRequest}
import com.pancakedb.spark.PancakeDataWriter.{HashedPartition, NWrittenPerInfo, PancakeCommitMessage}
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
  partitionFieldGetters: Array[PartitionFieldGetter],
  fieldGetters: Array[FieldGetter],
) extends DataWriter[InternalRow] {
  private val stagedRows = mutable.Map.empty[HashedPartition, ArrayBuffer[idl.Row]]
  private val logger = LoggerFactory.getLogger(getClass)
  private var nWritten = 0

  def makeHashedPartition(row: InternalRow): HashedPartition = {
    HashedPartition.fromTuples(partitionFieldGetters.map(getter => (getter.name, getter.get(row))))
  }

  def makePancakeRow(row: InternalRow): idl.Row = {
    val values = fieldGetters
      .map(getter => (getter.name, getter.get(row)))
      .filter({case (_, fv) =>
        fv.getValueCase match {
          case FieldValue.ValueCase.VALUE_NOT_SET => false
          case _ => true
        }
      })
      .toMap
    idl.Row.newBuilder()
      .putAllFields(values.asJava)
      .build()
  }

  private def flushPartition(partition: HashedPartition): Unit = {
    logger.debug(s"Task $taskId Spark partition $partitionId flushing write of ${stagedRows(partition).length} rows")
    val rows = stagedRows(partition)
    val req = WriteToPartitionRequest.newBuilder()
      .setTableName(params.tableName)
      .putAllPartition(partition.partitionValues.asJava)
      .addAllRows(rows.asJava)
      .build()
    stagedRows(partition) = ArrayBuffer.empty
    client.Api.writeToPartition(req)

    val newNWritten = nWritten + rows.length
    if (newNWritten / NWrittenPerInfo != nWritten / NWrittenPerInfo) {
      logger.info(s"Task $taskId Spark partition $partitionId has written $newNWritten rows and going")
    }
    nWritten = newNWritten
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
  private val NWrittenPerInfo = 50000

  case class PancakeCommitMessage() extends WriterCommitMessage

  case class HashedPartition(
    partitionValues: Map[String, PartitionFieldValue],
    hash: Int,
  ) {
    override def hashCode(): Int = hash

    override def equals(obj: Any): Boolean = {
      val other = obj.asInstanceOf[HashedPartition]
      other.hash == hash && partitionValues.equals(other.partitionValues)
    }
  }

  object HashedPartition {
    def fromTuples(partition: Array[(String, PartitionFieldValue)]): HashedPartition = {
      // improvise a hash fn
      var hash = 0
      partition.foreach({case (_, field) =>
        hash *= 1234577
        hash += field.hashCode()
      })
      HashedPartition(
        partition.toMap,
        hash,
      )
    }
  }
}
