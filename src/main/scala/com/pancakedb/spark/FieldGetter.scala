package com.pancakedb.spark

import com.pancakedb.idl.FieldValue
import org.apache.spark.sql.catalyst.InternalRow

private[spark] case class FieldGetter(name: String, get: InternalRow => FieldValue)
