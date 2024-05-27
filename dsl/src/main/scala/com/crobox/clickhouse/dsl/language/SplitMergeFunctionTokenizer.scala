package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait SplitMergeFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeSplitMergeFunction(col: SplitMergeFunction[_])(implicit ctx: TokenizeContext): String = col match {
    case SplitByChar(sep: StringColMagnet[_], col: StringColMagnet[_]) =>
      // Some small optimizations
      val separator = tokenizeColumn(sep.column)
      if (separator.length == 3) {
        val s = separator.charAt(1).toInt

        // https://en.wikipedia.org/wiki/List_of_Unicode_characters
        if (s >= 32 && s <= 126) {
          s"splitByChar($separator,${tokenizeColumn(col.column)})"
        } else {
          s"splitByChar(char($s),${tokenizeColumn(col.column)})"
        }
      } else {
        s"splitByString($separator,${tokenizeColumn(col.column)})"
      }
    case SplitByString(sep: StringColMagnet[_], col: StringColMagnet[_]) =>
      s"splitByString(${tokenizeColumn(sep.column)},${tokenizeColumn(col.column)})"
    case ArrayStringConcat(col: ArrayColMagnet[_], sep: StringColMagnet[_]) =>
      s"arrayStringConcat(${tokenizeColumn(col.column)},${tokenizeColumn(sep.column)})"
    case AlphaTokens(col: StringColMagnet[_]) => s"alphaTokens(${tokenizeColumn(col.column)})"
  }
}
