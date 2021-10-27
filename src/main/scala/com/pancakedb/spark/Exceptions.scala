package com.pancakedb.spark

import org.apache.spark.sql.types.DataType

object Exceptions {
  case class IncompatibleDataTypeException(dtype: DataType) extends Exception(
    s"${this.getClass.getSimpleName} dtype=$dtype"
  )
  case class TableDoesNotExist(name: String) extends Exception(
    s"Pancake table $name does not exist"
  )
  case class CorruptDataException(message: String) extends Exception(message)

  case object UnrecognizedDataTypeException extends Exception
  case object UnrecognizedPartitionDataTypeException extends Exception
}
