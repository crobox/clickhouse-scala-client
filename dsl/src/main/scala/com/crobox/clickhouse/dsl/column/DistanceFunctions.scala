package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

// TODO enforce numeric vectors
trait DistanceFunctions { self: Magnets =>

  sealed trait DistanceFunction
  abstract class DistanceFunctionOp[V] extends ExpressionColumn[V](EmptyColumn) with DistanceFunction

  // L1
  case class L1Norm[V](vector: ArrayColMagnet[V])                                   extends DistanceFunctionOp[V]
  case class L1Distance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V])  extends DistanceFunctionOp[V]
  case class L1Normalize[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]) extends DistanceFunctionOp[V]

  // L2
  case class L2Norm[V](vector: ArrayColMagnet[V])                                   extends DistanceFunctionOp[V]
  case class L2Distance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V])  extends DistanceFunctionOp[V]
  case class L2Normalize[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]) extends DistanceFunctionOp[V]

  // L2 Squared
  case class L2SquaredNorm[V](vector: ArrayColMagnet[V])                                  extends DistanceFunctionOp[V]
  case class L2SquaredDistance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]) extends DistanceFunctionOp[V]

  // LInf
  case class LInfNorm[V](vector: ArrayColMagnet[V])                                   extends DistanceFunctionOp[V]
  case class LInfDistance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V])  extends DistanceFunctionOp[V]
  case class LInfNormalize[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]) extends DistanceFunctionOp[V]

  // LP
  case class LPNorm[V](vector: ArrayColMagnet[V], p: Float) extends DistanceFunctionOp[V]
  case class LPDistance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V], p: Float)
      extends DistanceFunctionOp[V]
  case class LPNormalize[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V], p: Float)
      extends DistanceFunctionOp[V]

  // cosine
  case class CosineDistance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]) extends DistanceFunctionOp[V]

  def l1Norm[V](vector: ArrayColMagnet[V]): L1Norm[V] = L1Norm(vector)
  def l1Normalize[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]): L1Normalize[V] =
    L1Normalize(vector1, vector2)
  def l1Distance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]): L1Distance[V] =
    L1Distance(vector1, vector2)

  def l2Norm[V](vector: ArrayColMagnet[V]): L2Norm[V] = L2Norm(vector)
  def l2Normalize[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]): L2Normalize[V] =
    L2Normalize(vector1, vector2)
  def l2Distance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]): L2Distance[V] =
    L2Distance(vector1, vector2)

  def l2SquaredNorm[V](vector: ArrayColMagnet[V]): L2SquaredNorm[V] = L2SquaredNorm(vector)
  def l2SquaredDistance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]): L2SquaredDistance[V] =
    L2SquaredDistance(vector1, vector2)

  def lInfNorm[V](vector: ArrayColMagnet[V]): LInfNorm[V] = LInfNorm(vector)
  def lInfNormalize[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]): LInfNormalize[V] =
    LInfNormalize(vector1, vector2)
  def lInfDistance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]): LInfDistance[V] =
    LInfDistance(vector1, vector2)

  def lPNorm[V](vector: ArrayColMagnet[V], p: Float): LPNorm[V] = LPNorm(vector, p)
  def lPNormalize[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V], p: Float): LPNormalize[V] =
    LPNormalize(vector1, vector2, p)
  def lPDistance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V], p: Float): LPDistance[V] =
    LPDistance(vector1, vector2, p)

  def cosineDistance[V](vector1: ArrayColMagnet[V], vector2: ArrayColMagnet[V]): CosineDistance[V] =
    CosineDistance(vector1, vector2)

}
