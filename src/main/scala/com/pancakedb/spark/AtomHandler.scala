package com.pancakedb.spark

import com.pancakedb.client.RepLevelsColumn
import com.pancakedb.spark.Exceptions.CorruptDataException
import org.apache.spark.sql.execution.vectorized.WritableColumnVector

trait AtomHandler[A] {
  def appendAtomic(vector: WritableColumnVector, value: A): Unit
  val inherentNestingDepth: Int

  def fillVector(
    unsafeColumn: RepLevelsColumn[_],
    vector: WritableColumnVector,
  ): Unit = {
    val column = unsafeColumn.asInstanceOf[RepLevelsColumn[A]]
    var tempVector = vector
    val atomicDepth = column.nestingDepth + inherentNestingDepth
    val nestedVectors = (0 until atomicDepth).map(_ => {
      val res = tempVector
      tempVector = tempVector.arrayData()
      res
    })
    val lengths = Array.fill(atomicDepth)(0)
    val offsets = Array.fill(atomicDepth)(0)
    val flatVector = tempVector

    var atomIdx = 0
    var lastDepth = 0
    def closeArray(depth: Int): Unit = {
      val nestedVec = nestedVectors(depth)
      val rowId = nestedVec.appendNotNull()
      val len = lengths(depth)
      nestedVec.putArray(rowId, offsets(depth), len)
      offsets(depth) += len
      lengths(depth) = 0
      if (depth > 0) {
        lengths(depth - 1) += 1
      }
    }
    column.repLevels.foreach(l => {
      val depth = (l.toInt - 1).max(0)
      if (depth < atomicDepth && (l > 0 || lastDepth > 0)) {
        var depthIter = (lastDepth - 1).max(depth)
        while (depthIter >= depth) {
          closeArray(depthIter)
          depthIter -= 1
        }
      }

      if (l == 0) {
        vector.appendNull()
      } else if (depth == atomicDepth) {
        appendAtomic(flatVector, column.atoms(atomIdx))
        if (depth > 0) {
          lengths(depth - 1) += 1
        }
        atomIdx += 1
      } else if (depth > atomicDepth) {
        throw CorruptDataException(
          s"encountered rep level $l in column with atomic nesting depth $atomicDepth"
        )
      }

      lastDepth = depth
    })

    if (atomIdx < column.atoms.length) {
      throw CorruptDataException(
        s"reached end of column with only $atomIdx/${column.atoms.length} atoms"
      )
    }
  }
}
