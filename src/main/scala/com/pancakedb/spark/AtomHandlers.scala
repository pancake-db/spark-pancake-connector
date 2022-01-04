package com.pancakedb.spark
import org.apache.spark.sql.execution.vectorized.WritableColumnVector

object AtomHandlers {
  object LongHandler extends AtomHandler[Long] {
    override def appendAtomic(vector: WritableColumnVector, value: Long): Unit = {
      vector.appendLong(value)
    }
    override val inherentNestingDepth: Int = 0
  }

  object BooleanHandler extends AtomHandler[Boolean] {
    override def appendAtomic(vector: WritableColumnVector, value: Boolean): Unit = {
      vector.appendBoolean(value)
    }
    override val inherentNestingDepth: Int = 0
  }

  object FloatHandler extends AtomHandler[Float] {
    override def appendAtomic(vector: WritableColumnVector, value: Float): Unit = {
      vector.appendFloat(value)
    }
    override val inherentNestingDepth: Int = 0
  }

  object DoubleHandler extends AtomHandler[Double] {
    override def appendAtomic(vector: WritableColumnVector, value: Double): Unit = {
      vector.appendDouble(value)
    }
    override val inherentNestingDepth: Int = 0
  }

  object ByteHandler extends AtomHandler[Byte] {
    override def appendAtomic(vector: WritableColumnVector, value: Byte): Unit = {
      vector.appendByte(value)
    }
    // byte is always grouped into string or bytes
    override val inherentNestingDepth: Int = 1
  }
}
