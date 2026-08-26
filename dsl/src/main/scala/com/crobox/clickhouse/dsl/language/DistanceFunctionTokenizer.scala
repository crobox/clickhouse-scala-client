package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait DistanceFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeDistanceFunction(col: DistanceFunction)(implicit ctx: TokenizeContext): String = col match {

    // cosine
    case CosineDistance(vector1, vector2) =>
      s"cosineDistance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"

    // L1
    case L1Norm(vector)               => s"L1Norm(${tokenizeColumn(vector.column)})"
    case L1Normalize(vector)          => s"L1Normalize(${tokenizeColumn(vector.column)})"
    case L1Distance(vector1, vector2) =>
      s"L1Distance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"

    // L2
    case L2Norm(vector)               => s"L2Norm(${tokenizeColumn(vector.column)})"
    case L2Normalize(vector)          => s"L2Normalize(${tokenizeColumn(vector.column)})"
    case L2Distance(vector1, vector2) =>
      s"L2Distance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"

    // L2Squared
    case L2SquaredNorm(vector)               => s"L2SquaredNorm(${tokenizeColumn(vector.column)})"
    case L2SquaredDistance(vector1, vector2) =>
      s"L2SquaredDistance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"

    // LInf
    case LInfNorm(vector)               => s"LinfNorm(${tokenizeColumn(vector.column)})"
    case LInfNormalize(vector)          => s"LinfNormalize(${tokenizeColumn(vector.column)})"
    case LInfDistance(vector1, vector2) =>
      s"LinfDistance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)})"

    // LP
    case LPNorm(vector, p)                      => s"LpNorm(${tokenizeColumn(vector.column)}, $p)"
    case LPNormalize(vector, p: Float)          => s"LpNormalize(${tokenizeColumn(vector.column)}, $p)"
    case LPDistance(vector1, vector2, p: Float) =>
      s"LpDistance(${tokenizeColumn(vector1.column)}, ${tokenizeColumn(vector2.column)}, $p)"
  }
}
