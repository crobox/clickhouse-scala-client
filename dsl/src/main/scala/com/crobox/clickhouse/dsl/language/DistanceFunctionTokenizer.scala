package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait DistanceFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeDistanceFunction(col: DistanceFunction)(implicit ctx: TokenizeContext): String = col match {
    case CosineDistance(vector1: ArrayColMagnet[_], vector2: ArrayColMagnet[_]) =>
      s"cosineDistance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"

    case L1Norm(vector: ArrayColMagnet[_]) => s"L1Norm(${tokenizeColumn(vector.column)})"
    case L1Normalize(vector1: ArrayColMagnet[_], vector2: ArrayColMagnet[_]) =>
      s"L1Normalize(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"
    case L1Distance(vector1: ArrayColMagnet[_], vector2: ArrayColMagnet[_]) =>
      s"L1Distance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"

    case L2Norm(vector: ArrayColMagnet[_]) => s"L2Norm(${tokenizeColumn(vector.column)})"
    case L2Normalize(vector1: ArrayColMagnet[_], vector2: ArrayColMagnet[_]) =>
      s"L2Normalize(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"
    case L2Distance(vector1: ArrayColMagnet[_], vector2: ArrayColMagnet[_]) =>
      s"L2Distance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"

    case L2SquaredNorm(vector: ArrayColMagnet[_]) => s"L2SquaredNorm(${tokenizeColumn(vector.column)})"
    case L2SquaredDistance(vector1: ArrayColMagnet[_], vector2: ArrayColMagnet[_]) =>
      s"L2SquaredDistance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"

    case LInfNorm(vector: ArrayColMagnet[_]) => s"LInfNorm(${tokenizeColumn(vector.column)})"
    case LInfNormalize(vector1: ArrayColMagnet[_], vector2: ArrayColMagnet[_]) =>
      s"LinfNormalize(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"
    case LInfDistance(vector1: ArrayColMagnet[_], vector2: ArrayColMagnet[_]) =>
      s"LinfDistance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"

    case LPNorm(vector: ArrayColMagnet[_], p) => s"LpNorm(${tokenizeColumn(vector.column)}, $p)"
    case LPNormalize(vector1: ArrayColMagnet[_], vector2: ArrayColMagnet[_], p: Float) =>
      s"LpNormalize(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)}, $p)"
    case LPDistance(vector1: ArrayColMagnet[_], vector2: ArrayColMagnet[_], p: Float) =>
      s"LpDistance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)}, $p)"
  }
}
