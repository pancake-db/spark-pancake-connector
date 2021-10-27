package com.pancakedb.spark

import com.pancakedb.client.RepLevelsColumn
import com.pancakedb.spark.AtomHandlers.{ByteHandler, LongHandler}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{ArrayType, DataTypes}
import org.apache.spark.sql.vectorized.ColumnarArray

class AtomHandlerTest extends TestBase {
  "fillVector" should "handle flat cases" in {
    val n = 5
    val column = RepLevelsColumn(
      Array(1L, 2L, 3L),
      Array(0, 1, 1, 0, 1),
      0,
      n,
    )
    val vector = new OnHeapColumnVector(n, DataTypes.LongType)
    LongHandler.fillVector(column, vector)
    assertResult(n)(vector.getElementsAppended)
    assert(vector.isNullAt(0))
    assertResult(1L)(vector.getLong(1))
    assertResult(2L)(vector.getLong(2))
    assert(vector.isNullAt(3))
    assertResult(3L)(vector.getLong(4))
  }

  "fillVector" should "handle nested cases" in {
    val n = 5
    /* once-nested strings, so twice-nested bytes
    [
      [],
      [[]],
      [[11]],
      null,
      [[12, 13], [14]]
    ]
     */
    val column = RepLevelsColumn(
      Array(10, 11, 12, 13).map(_.toByte),
      Array(1, 2, 1, 3, 0, 3, 3, 2, 3, 1),
      1,
      n
    )
    val vector = new OnHeapColumnVector(
      n,
      ArrayType(ArrayType(DataTypes.ByteType))
    )
    ByteHandler.fillVector(column, vector)
    assertResult(n)(vector.getElementsAppended)
    assertResult(0)(vector.getArray(0).numElements())
    assertResult(1)(vector.getArray(1).numElements())
    assertResult(0)(vector.getArray(1).getArray(0).numElements())
    assertResult(1)(vector.getArray(2).numElements)
    assertResult(1)(vector.getArray(2).getArray(0).numElements)
    assertResult(10)(vector.getArray(2).getArray(0).getByte(0))
    assertResult(null)(vector.getArray(3))
    assertResult(2)(vector.getArray(4).numElements())
    assertResult(2)(vector.getArray(4).getArray(0).numElements())
    assertResult(11)(vector.getArray(4).getArray(0).getByte(0))
    assertResult(12)(vector.getArray(4).getArray(0).getByte(1))
    assertResult(1)(vector.getArray(4).getArray(1).numElements())
    assertResult(13)(vector.getArray(4).getArray(1).getByte(0))
  }

  "fillVector" should "handle nested cases with leading null" in {
    val n = 2
    // once-nested strings, so twice-nested bytes
    val column = RepLevelsColumn(
      Array.empty[Byte],
      Array(0, 0),
      1,
      n
    )
    val vector = new OnHeapColumnVector(
      n,
      ArrayType(ArrayType(DataTypes.ByteType))
    )
    ByteHandler.fillVector(column, vector)
    assertResult(n)(vector.getElementsAppended)
    assertResult(null)(vector.getArray(0))
    assertResult(null)(vector.getArray(1))
  }
}
