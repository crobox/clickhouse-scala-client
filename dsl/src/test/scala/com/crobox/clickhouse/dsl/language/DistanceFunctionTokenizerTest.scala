package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._

class DistanceFunctionTokenizerTest extends DslTestSpec {

  private val array1: Array[Int]  = Array(1)
  private val array12: Array[Int] = Array(1, 2)
  private val tuple1: Tuple       = Tuple(Seq(1).map(const(_)))
  private val tuple12: Tuple      = Tuple(Seq(1, 2).map(const(_)))
  private val p                   = 1.0f

  behavior of "L1Norm"
  it should "tokenize L1Norm " in {
    toSQL(select(l1Norm(array1))) should matchSQL(s"SELECT L1Norm([1])")
    toSQL(select(l1Norm(array12))) should matchSQL(s"SELECT L1Norm([1, 2])")
    toSQL(select(l1Norm(tuple1))) should matchSQL(s"SELECT L1Norm((1))")
    toSQL(select(l1Norm(tuple12))) should matchSQL(s"SELECT L1Norm((1, 2))")
  }
  // TODO should fail
  ignore should "tokenize L1Norm with String Tuples" in {
    toSQL(select(L1Norm(Tuple(Seq(const("A")))))) should matchSQL(s"SELECT L1Norm(('A'))")
    toSQL(select(L1Norm(Tuple(Seq(const("a"), const("b")))))) should matchSQL(s"SELECT L1Norm(('a', 'b'))")
  }

  behavior of "L2Norm"
  it should "tokenize L2Norm " in {
    toSQL(select(l2Norm(array1))) should matchSQL(s"SELECT L2Norm([1])")
    toSQL(select(l2Norm(array12))) should matchSQL(s"SELECT L2Norm([1, 2])")
    toSQL(select(l2Norm(tuple1))) should matchSQL(s"SELECT L2Norm((1))")
    toSQL(select(l2Norm(tuple12))) should matchSQL(s"SELECT L2Norm((1, 2))")
  }

  behavior of "L2SquaredNorm"
  it should "tokenize L2SquaredNorm " in {
    toSQL(select(l2SquaredNorm(array1))) should matchSQL(s"SELECT L2SquaredNorm([1])")
    toSQL(select(l2SquaredNorm(array12))) should matchSQL(s"SELECT L2SquaredNorm([1, 2])")
    toSQL(select(l2SquaredNorm(tuple1))) should matchSQL(s"SELECT L2SquaredNorm((1))")
    toSQL(select(l2SquaredNorm(tuple12))) should matchSQL(s"SELECT L2SquaredNorm((1, 2))")
  }

  behavior of "LInfNorm"
  it should "tokenize L2InfNorm " in {
    toSQL(select(lInfNorm(array1))) should matchSQL(s"SELECT LInfNorm([1])")
    toSQL(select(lInfNorm(array12))) should matchSQL(s"SELECT LInfNorm([1, 2])")
    toSQL(select(lInfNorm(tuple1))) should matchSQL(s"SELECT LInfNorm((1))")
    toSQL(select(lInfNorm(tuple12))) should matchSQL(s"SELECT LInfNorm((1, 2))")
  }

  behavior of "LPNorm"
  it should "tokenize L2InfNorm " in {
    toSQL(select(lPNorm(array1, 1.0f))) should matchSQL(s"SELECT LpNorm([1], 1.0)")
    toSQL(select(lPNorm(tuple1, 1.0f))) should matchSQL(s"SELECT LpNorm((1), 1.0)")
    toSQL(select(lPNorm(tuple12, 1.0f))) should matchSQL(s"SELECT LpNorm((1, 2), 1.0)")
  }

  behavior of "L1Distance"
  it should "tokenize L1Distance " in {
    toSQL(select(l1Distance(array1, array1))) should matchSQL(s"SELECT L1Distance([1], [1])")
    toSQL(select(l1Distance(array12, array12))) should matchSQL(s"SELECT L1Distance([1, 2], [1, 2])")
    toSQL(select(l1Distance(tuple1, tuple1))) should matchSQL(s"SELECT L1Distance((1), (1))")
    toSQL(select(l1Distance(tuple12, tuple12))) should matchSQL(s"SELECT L1Distance((1, 2), (1, 2))")
  }

  behavior of "L2Distance"
  it should "tokenize L2Distance " in {
    toSQL(select(l2Distance(array1, array1))) should matchSQL(s"SELECT L2Distance([1], [1])")
    toSQL(select(l2Distance(array12, array12))) should matchSQL(s"SELECT L2Distance([1, 2], [1, 2])")
    toSQL(select(l2Distance(tuple1, tuple1))) should matchSQL(s"SELECT L2Distance((1), (1))")
    toSQL(select(l2Distance(tuple12, tuple12))) should matchSQL(s"SELECT L2Distance((1, 2), (1, 2))")
  }

  behavior of "L2SquaredDistance"
  it should "tokenize L2SquaredDistance " in {
    toSQL(select(l2SquaredDistance(array1, array1))) should matchSQL(s"SELECT L2SquaredDistance([1], [1])")
    toSQL(select(l2SquaredDistance(array12, array12))) should matchSQL(s"SELECT L2SquaredDistance([1, 2], [1, 2])")
    toSQL(select(l2SquaredDistance(tuple1, tuple1))) should matchSQL(s"SELECT L2SquaredDistance((1), (1))")
    toSQL(select(l2SquaredDistance(tuple12, tuple12))) should matchSQL(s"SELECT L2SquaredDistance((1, 2), (1, 2))")
  }

  behavior of "LinfDistance"
  it should "tokenize LinfDistance " in {
    toSQL(select(lInfDistance(array1, array1))) should matchSQL(s"SELECT LinfDistance([1], [1])")
    toSQL(select(lInfDistance(array12, array12))) should matchSQL(s"SELECT LinfDistance([1, 2], [1, 2])")
    toSQL(select(lInfDistance(tuple1, tuple1))) should matchSQL(s"SELECT LinfDistance((1), (1))")
    toSQL(select(lInfDistance(tuple12, tuple12))) should matchSQL(s"SELECT LinfDistance((1, 2), (1, 2))")
  }

  behavior of "LpDistance"
  it should "tokenize LpDistance " in {
    toSQL(select(lPDistance(array1, array1, p))) should matchSQL(s"SELECT LpDistance([1], [1], 1.0)")
    toSQL(select(lPDistance(array12, array12, p))) should matchSQL(s"SELECT LpDistance([1, 2], [1, 2], 1.0)")
    toSQL(select(lPDistance(tuple1, tuple1, p))) should matchSQL(s"SELECT LpDistance((1), (1), 1.0)")
    toSQL(select(lPDistance(tuple12, tuple12, p))) should matchSQL(s"SELECT LpDistance((1, 2), (1, 2), 1.0)")
  }

  behavior of "cosineDistance"
  it should "tokenize cosineDistance " in {
    toSQL(select(cosineDistance(array1, array1))) should matchSQL(s"SELECT cosineDistance([1], [1])")
    toSQL(select(cosineDistance(array12, array12))) should matchSQL(s"SELECT cosineDistance([1, 2], [1, 2])")
    toSQL(select(cosineDistance(tuple1, tuple1))) should matchSQL(s"SELECT cosineDistance((1), (1))")
    toSQL(select(cosineDistance(tuple12, tuple12))) should matchSQL(s"SELECT cosineDistance((1, 2), (1, 2))")
  }

  behavior of "L1Normalize"
  it should "tokenize L1Normalize " in {
    toSQL(select(l1Normalize(array1, array1))) should matchSQL(s"SELECT L1Normalize([1], [1])")
    toSQL(select(l1Normalize(array12, array12))) should matchSQL(s"SELECT L1Normalize([1, 2], [1, 2])")
    toSQL(select(l1Normalize(tuple1, tuple1))) should matchSQL(s"SELECT L1Normalize((1), (1))")
    toSQL(select(l1Normalize(tuple12, tuple12))) should matchSQL(s"SELECT L1Normalize((1, 2), (1, 2))")
  }

  behavior of "L2Normalize"
  it should "tokenize L2Normalize " in {
    toSQL(select(l2Normalize(array1, array1))) should matchSQL(s"SELECT L2Normalize([1], [1])")
    toSQL(select(l2Normalize(array12, array12))) should matchSQL(s"SELECT L2Normalize([1, 2], [1, 2])")
    toSQL(select(l2Normalize(tuple1, tuple1))) should matchSQL(s"SELECT L2Normalize((1), (1))")
    toSQL(select(l2Normalize(tuple12, tuple12))) should matchSQL(s"SELECT L2Normalize((1, 2), (1, 2))")
  }

  behavior of "LinfNormalize"
  it should "tokenize LinfNormalize " in {
    toSQL(select(lInfNormalize(array1, array1))) should matchSQL(s"SELECT LinfNormalize([1], [1])")
    toSQL(select(lInfNormalize(array12, array12))) should matchSQL(s"SELECT LinfNormalize([1, 2], [1, 2])")
    toSQL(select(lInfNormalize(tuple1, tuple1))) should matchSQL(s"SELECT LinfNormalize((1), (1))")
    toSQL(select(lInfNormalize(tuple12, tuple12))) should matchSQL(s"SELECT LinfNormalize((1, 2), (1, 2))")
  }

  behavior of "LpNormalize"
  it should "tokenize LpNormalize " in {
    toSQL(select(lPNormalize(array1, array1, p))) should matchSQL(s"SELECT LpNormalize([1], [1], 1.0)")
    toSQL(select(lPNormalize(array12, array12, p))) should matchSQL(s"SELECT LpNormalize([1, 2], [1, 2], 1.0)")
    toSQL(select(lPNormalize(tuple1, tuple1, p))) should matchSQL(s"SELECT LpNormalize((1), (1), 1.0)")
    toSQL(select(lPNormalize(tuple12, tuple12, p))) should matchSQL(s"SELECT LpNormalize((1, 2), (1, 2), 1.0)")
  }
}
