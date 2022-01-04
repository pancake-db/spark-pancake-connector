package com.pancakedb.spark

import com.pancakedb.idl.PartitionFieldValue
import org.apache.spark.sql.catalyst.InternalRow

private[spark] case class PartitionFieldGetter(name: String, get: InternalRow => PartitionFieldValue)
