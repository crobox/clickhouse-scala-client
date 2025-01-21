package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType

class DistanceFunctionTokenizerTest extends DslTestSpec {

  private val array1: Array[Int]  = Array(1)
  private val array12: Array[Int] = Array(1, 2)
  private val tuple1: Tuple       = Tuple(Seq(1).map(const(_)))
  private val tuple12: Tuple      = Tuple(Seq(1, 2).map(const(_)))
  private val numbers2            = NativeColumn[Seq[Int]]("numbers2", ColumnType.Array(ColumnType.UInt32))
  private val p                   = 1.0f

  behavior of "DistanceFunctionTokenizer"

  it should "tokenize L1Norm " in {
    toSQL(select(l1Norm(array1))) should matchSQL(s"SELECT L1Norm([1])")
    toSQL(select(l1Norm(array12))) should matchSQL(s"SELECT L1Norm([1, 2])")
    toSQL(select(l1Norm[Int](tuple1))) should matchSQL(s"SELECT L1Norm((1))")
    toSQL(select(l1Norm[Int](tuple12))) should matchSQL(s"SELECT L1Norm((1, 2))")
    toSQL(select(l1Norm(numbers)).from(OneTestTable)) should matchSQL(
      s"SELECT L1Norm(numbers) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize L1Normalize " in {
    toSQL(select(l1Normalize(array1, array1))) should matchSQL(s"SELECT L1Normalize([1], [1])")
    toSQL(select(l1Normalize(array12, array12))) should matchSQL(s"SELECT L1Normalize([1, 2], [1, 2])")
    toSQL(select(l1Normalize[Int](tuple1, tuple1))) should matchSQL(s"SELECT L1Normalize((1), (1))")
    toSQL(select(l1Normalize[Int](tuple12, tuple12))) should matchSQL(s"SELECT L1Normalize((1, 2), (1, 2))")
    toSQL(select(l1Normalize(numbers, numbers2)).from(OneTestTable)) should matchSQL(
      s"SELECT L1Normalize(numbers, numbers2) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize L1Distance " in {
    toSQL(select(l1Distance(array1, array1))) should matchSQL(s"SELECT L1Distance([1], [1])")
    toSQL(select(l1Distance(array12, array12))) should matchSQL(s"SELECT L1Distance([1, 2], [1, 2])")
    toSQL(select(l1Distance[Int](tuple1, tuple1))) should matchSQL(s"SELECT L1Distance((1), (1))")
    toSQL(select(l1Distance[Int](tuple12, tuple12))) should matchSQL(s"SELECT L1Distance((1, 2), (1, 2))")
    toSQL(select(l1Distance(numbers, numbers2)).from(OneTestTable)) should matchSQL(
      s"SELECT L1Distance(numbers, numbers2) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize L2Norm " in {
    toSQL(select(l2Norm(array1))) should matchSQL(s"SELECT L2Norm([1])")
    toSQL(select(l2Norm(array12))) should matchSQL(s"SELECT L2Norm([1, 2])")
    toSQL(select(l2Norm[Int](tuple1))) should matchSQL(s"SELECT L2Norm((1))")
    toSQL(select(l2Norm[Int](tuple12))) should matchSQL(s"SELECT L2Norm((1, 2))")
    toSQL(select(l2Norm(numbers)).from(OneTestTable)) should matchSQL(
      s"SELECT L2Norm(numbers) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize L2Normalize " in {
    toSQL(select(l2Normalize(array1, array1))) should matchSQL(s"SELECT L2Normalize([1], [1])")
    toSQL(select(l2Normalize(array12, array12))) should matchSQL(s"SELECT L2Normalize([1, 2], [1, 2])")
    toSQL(select(l2Normalize[Int](tuple1, tuple1))) should matchSQL(s"SELECT L2Normalize((1), (1))")
    toSQL(select(l2Normalize[Int](tuple12, tuple12))) should matchSQL(s"SELECT L2Normalize((1, 2), (1, 2))")
    toSQL(select(l2Normalize(numbers, numbers2)).from(OneTestTable)) should matchSQL(
      s"SELECT L2Normalize(numbers, numbers2) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize L2Distance " in {
    toSQL(select(l2Distance(array1, array1))) should matchSQL(s"SELECT L2Distance([1], [1])")
    toSQL(select(l2Distance(array12, array12))) should matchSQL(s"SELECT L2Distance([1, 2], [1, 2])")
    toSQL(select(l2Distance[Int](tuple1, tuple1))) should matchSQL(s"SELECT L2Distance((1), (1))")
    toSQL(select(l2Distance[Int](tuple12, tuple12))) should matchSQL(s"SELECT L2Distance((1, 2), (1, 2))")
    toSQL(select(l2Distance(numbers, numbers2)).from(OneTestTable)) should matchSQL(
      s"SELECT L2Distance(numbers, numbers2) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize L2SquaredNorm " in {
    toSQL(select(l2SquaredNorm(array1))) should matchSQL(s"SELECT L2SquaredNorm([1])")
    toSQL(select(l2SquaredNorm(array12))) should matchSQL(s"SELECT L2SquaredNorm([1, 2])")
    toSQL(select(l2SquaredNorm[Int](tuple1))) should matchSQL(s"SELECT L2SquaredNorm((1))")
    toSQL(select(l2SquaredNorm[Int](tuple12))) should matchSQL(s"SELECT L2SquaredNorm((1, 2))")
    toSQL(select(l2SquaredNorm(numbers)).from(OneTestTable)) should matchSQL(
      s"SELECT L2SquaredNorm(numbers) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize L2SquaredDistance " in {
    toSQL(select(l2SquaredDistance(array1, array1))) should matchSQL(s"SELECT L2SquaredDistance([1], [1])")
    toSQL(select(l2SquaredDistance(array12, array12))) should matchSQL(s"SELECT L2SquaredDistance([1, 2], [1, 2])")
    toSQL(select(l2SquaredDistance[Int](tuple1, tuple1))) should matchSQL(s"SELECT L2SquaredDistance((1), (1))")
    toSQL(select(l2SquaredDistance[Int](tuple12, tuple12))) should matchSQL(s"SELECT L2SquaredDistance((1, 2), (1, 2))")
    toSQL(select(l2SquaredDistance(numbers, numbers2)).from(OneTestTable)) should matchSQL(
      s"SELECT L2SquaredDistance(numbers, numbers2) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize L2InfNorm " in {
    toSQL(select(lInfNorm(array1))) should matchSQL(s"SELECT LinfNorm([1])")
    toSQL(select(lInfNorm(array12))) should matchSQL(s"SELECT LinfNorm([1, 2])")
    toSQL(select(lInfNorm[Int](tuple1))) should matchSQL(s"SELECT LinfNorm((1))")
    toSQL(select(lInfNorm[Int](tuple12))) should matchSQL(s"SELECT LinfNorm((1, 2))")
    toSQL(select(lInfNorm(numbers)).from(OneTestTable)) should matchSQL(
      s"SELECT LinfNorm(numbers) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize LinfNormalize " in {
    toSQL(select(lInfNormalize(array1, array1))) should matchSQL(s"SELECT LinfNormalize([1], [1])")
    toSQL(select(lInfNormalize(array12, array12))) should matchSQL(s"SELECT LinfNormalize([1, 2], [1, 2])")
    toSQL(select(lInfNormalize(tuple1, tuple1))) should matchSQL(s"SELECT LinfNormalize((1), (1))")
    toSQL(select(lInfNormalize(tuple12, tuple12))) should matchSQL(s"SELECT LinfNormalize((1, 2), (1, 2))")
    toSQL(select(lInfNormalize(numbers, numbers2)).from(OneTestTable)) should matchSQL(
      s"SELECT LinfNormalize(numbers, numbers2) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize LinfDistance " in {
    toSQL(select(lInfDistance(array1, array1))) should matchSQL(s"SELECT LinfDistance([1], [1])")
    toSQL(select(lInfDistance(array12, array12))) should matchSQL(s"SELECT LinfDistance([1, 2], [1, 2])")
    toSQL(select(lInfDistance[Int](tuple1, tuple1))) should matchSQL(s"SELECT LinfDistance((1), (1))")
    toSQL(select(lInfDistance[Int](tuple12, tuple12))) should matchSQL(s"SELECT LinfDistance((1, 2), (1, 2))")
    toSQL(select(lInfDistance(numbers, numbers2)).from(OneTestTable)) should matchSQL(
      s"SELECT LinfDistance(numbers, numbers2) FROM ${OneTestTable.quoted}"
    )
  }

  it should "tokenize LpNorm " in {
    toSQL(select(lPNorm(array1, 1.0f))) should matchSQL(s"SELECT LpNorm([1], 1.0)")
    toSQL(select(lPNorm[Int](tuple1, 1.0f))) should matchSQL(s"SELECT LpNorm((1), 1.0)")
    toSQL(select(lPNorm[Int](tuple12, 1.0f))) should matchSQL(s"SELECT LpNorm((1, 2), 1.0)")
    toSQL(select(lPNorm[Int](numbers, 1.0f))) should matchSQL(s"SELECT LpNorm(numbers, 1.0)")
  }

  it should "tokenize LpNormalize " in {
    toSQL(select(lPNormalize(array1, array1, p))) should matchSQL(s"SELECT LpNormalize([1], [1], 1.0)")
    toSQL(select(lPNormalize(array12, array12, p))) should matchSQL(s"SELECT LpNormalize([1, 2], [1, 2], 1.0)")
    toSQL(select(lPNormalize(tuple1, tuple1, p))) should matchSQL(s"SELECT LpNormalize((1), (1), 1.0)")
    toSQL(select(lPNormalize(tuple12, tuple12, p))) should matchSQL(s"SELECT LpNormalize((1, 2), (1, 2), 1.0)")
    toSQL(select(lPNormalize(numbers, numbers2, p))) should matchSQL(s"SELECT LpNormalize(numbers, numbers2, 1.0)")
  }

  it should "tokenize LpDistance " in {
    toSQL(select(lPDistance(array1, array1, p))) should matchSQL(s"SELECT LpDistance([1], [1], 1.0)")
    toSQL(select(lPDistance(array12, array12, p))) should matchSQL(s"SELECT LpDistance([1, 2], [1, 2], 1.0)")
    toSQL(select(lPDistance[Int](tuple1, tuple1, p))) should matchSQL(s"SELECT LpDistance((1), (1), 1.0)")
    toSQL(select(lPDistance[Int](tuple12, tuple12, p))) should matchSQL(s"SELECT LpDistance((1, 2), (1, 2), 1.0)")
    toSQL(select(lPDistance[Int](numbers, numbers2, p))) should matchSQL(s"SELECT LpDistance(numbers, numbers2, 1.0)")
  }

  it should "tokenize cosineDistance " in {
    toSQL(select(cosineDistance(array1, array1))) should matchSQL(s"SELECT cosineDistance([1], [1])")
    toSQL(select(cosineDistance(array12, array12))) should matchSQL(s"SELECT cosineDistance([1, 2], [1, 2])")
    toSQL(select(cosineDistance[Int](tuple1, tuple1))) should matchSQL(s"SELECT cosineDistance((1), (1))")
    toSQL(select(cosineDistance[Int](tuple12, tuple12))) should matchSQL(s"SELECT cosineDistance((1, 2), (1, 2))")
    toSQL(select(cosineDistance[Int](numbers, numbers2))) should matchSQL(s"SELECT cosineDistance(numbers, numbers2)")
  }

  it should "fail for non-numerical values" in {
    val col1 = NativeColumn[String]("column_1")
    val col2 = NativeColumn[Int]("column_2", ColumnType.UInt32)
    assertDoesNotCompile("""toSQL(select(T1Norm(Array("1", "2"))))""".stripMargin)
    assertDoesNotCompile("""toSQL(select(l1Norm(col1)).from(OneTestTable))""".stripMargin)
    assertDoesNotCompile("""toSQL(select(l1Norm(col2)).from(OneTestTable))""".stripMargin)
  }

}
