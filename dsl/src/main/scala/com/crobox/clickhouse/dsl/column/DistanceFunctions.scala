package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

trait DistanceFunctions { self: Magnets =>

  sealed trait DistanceFunction
  abstract class DistanceFunctionOp[V] extends ExpressionColumn[V](EmptyColumn) with DistanceFunction

  // The *Normalize functions are not shaped like the rest of the family: the server gives them one argument, not
  // two, and it must be a Tuple rather than an Array -- an Array is rejected with ILLEGAL_TYPE_OF_ARGUMENT. They
  // return a Tuple as well, hence Nothing, the same type Tuple itself carries.

  // L1
  case class L1Norm[V](vector: ArrayColMagnet[_ <: Iterable[V]])(implicit evidence: V => NumericCol[V])
      extends DistanceFunctionOp[V]
  case class L1Normalize(vector: ConstOrColMagnet[_]) extends DistanceFunctionOp[Nothing]
  case class L1Distance[V](vector1: ArrayColMagnet[_ <: Iterable[V]], vector2: ArrayColMagnet[_ <: Iterable[V]])(
      implicit evidence: V => NumericCol[V]
  ) extends DistanceFunctionOp[V]

  // L2
  case class L2Norm[V](vector: ArrayColMagnet[_ <: Iterable[V]])(implicit evidence: V => NumericCol[V])
      extends DistanceFunctionOp[V]
  case class L2Normalize(vector: ConstOrColMagnet[_]) extends DistanceFunctionOp[Nothing]
  case class L2Distance[V](vector1: ArrayColMagnet[_ <: Iterable[V]], vector2: ArrayColMagnet[_ <: Iterable[V]])(
      implicit evidence: V => NumericCol[V]
  ) extends DistanceFunctionOp[V]

  // L2 Squared
  case class L2SquaredNorm[V](vector: ArrayColMagnet[_ <: Iterable[V]])(implicit evidence: V => NumericCol[V])
      extends DistanceFunctionOp[V]
  case class L2SquaredDistance[V](vector1: ArrayColMagnet[_ <: Iterable[V]], vector2: ArrayColMagnet[_ <: Iterable[V]])(
      implicit evidence: V => NumericCol[V]
  ) extends DistanceFunctionOp[V]

  // LInf
  case class LInfNorm[V](vector: ArrayColMagnet[_ <: Iterable[V]])(implicit evidence: V => NumericCol[V])
      extends DistanceFunctionOp[V]
  case class LInfNormalize(vector: ConstOrColMagnet[_]) extends DistanceFunctionOp[Nothing]
  case class LInfDistance[V](vector1: ArrayColMagnet[_ <: Iterable[V]], vector2: ArrayColMagnet[_ <: Iterable[V]])(
      implicit evidence: V => NumericCol[V]
  ) extends DistanceFunctionOp[V]

  // LP
  case class LPNorm[V](vector: ArrayColMagnet[_ <: Iterable[V]], p: Float)(implicit evidence: V => NumericCol[V])
      extends DistanceFunctionOp[V]
  case class LPNormalize(vector: ConstOrColMagnet[_], p: Float) extends DistanceFunctionOp[Nothing]
  case class LPDistance[V](
      vector1: ArrayColMagnet[_ <: Iterable[V]],
      vector2: ArrayColMagnet[_ <: Iterable[V]],
      p: Float
  )(implicit
      evidence: V => NumericCol[V]
  ) extends DistanceFunctionOp[V]

  // cosine
  case class CosineDistance[V](
      vector1: ArrayColMagnet[_ <: Iterable[V]],
      vector2: ArrayColMagnet[_ <: Iterable[V]]
  )(implicit
      evidence: V => NumericCol[V]
  ) extends DistanceFunctionOp[V]

  // utilities
  def l1Norm[V](vector: ArrayColMagnet[_ <: Iterable[V]])(implicit evidence: V => NumericCol[V]): L1Norm[V] =
    L1Norm(vector)

  def l1Normalize(vector: ConstOrColMagnet[_]): L1Normalize = L1Normalize(vector)

  def l1Distance[V](vector1: ArrayColMagnet[_ <: Iterable[V]], vector2: ArrayColMagnet[_ <: Iterable[V]])(implicit
      evidence: V => NumericCol[V]
  ): L1Distance[V] = L1Distance(vector1, vector2)

  def l2Norm[V](vector: ArrayColMagnet[_ <: Iterable[V]])(implicit evidence: V => NumericCol[V]): L2Norm[V] =
    L2Norm(vector)

  def l2Normalize(vector: ConstOrColMagnet[_]): L2Normalize = L2Normalize(vector)

  def l2Distance[V](vector1: ArrayColMagnet[_ <: Iterable[V]], vector2: ArrayColMagnet[_ <: Iterable[V]])(implicit
      evidence: V => NumericCol[V]
  ): L2Distance[V] = L2Distance(vector1, vector2)

  def l2SquaredNorm[V](vector: ArrayColMagnet[_ <: Iterable[V]])(implicit
      evidence: V => NumericCol[V]
  ): L2SquaredNorm[V] = L2SquaredNorm(vector)

  def l2SquaredDistance[V](vector1: ArrayColMagnet[_ <: Iterable[V]], vector2: ArrayColMagnet[_ <: Iterable[V]])(
      implicit evidence: V => NumericCol[V]
  ): L2SquaredDistance[V] = L2SquaredDistance(vector1, vector2)

  def lInfNorm[V](vector: ArrayColMagnet[_ <: Iterable[V]])(implicit evidence: V => NumericCol[V]): LInfNorm[V] =
    LInfNorm(vector)

  def lInfNormalize(vector: ConstOrColMagnet[_]): LInfNormalize = LInfNormalize(vector)

  def lInfDistance[V](vector1: ArrayColMagnet[_ <: Iterable[V]], vector2: ArrayColMagnet[_ <: Iterable[V]])(implicit
      evidence: V => NumericCol[V]
  ): LInfDistance[V] = LInfDistance(vector1, vector2)

  def lPNorm[V](vector: ArrayColMagnet[_ <: Iterable[V]], p: Float)(implicit evidence: V => NumericCol[V]): LPNorm[V] =
    LPNorm(vector, p)

  def lPNormalize(vector: ConstOrColMagnet[_], p: Float): LPNormalize = LPNormalize(vector, p)

  def lPDistance[V](
      vector1: ArrayColMagnet[_ <: Iterable[V]],
      vector2: ArrayColMagnet[_ <: Iterable[V]],
      p: Float
  )(implicit evidence: V => NumericCol[V]): LPDistance[V] = LPDistance(vector1, vector2, p)

  def cosineDistance[V](
      vector1: ArrayColMagnet[_ <: Iterable[V]],
      vector2: ArrayColMagnet[_ <: Iterable[V]]
  )(implicit evidence: V => NumericCol[V]): CosineDistance[V] = CosineDistance(vector1, vector2)

}
