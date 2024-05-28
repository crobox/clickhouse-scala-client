package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait SplitMergeFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeSplitMergeFunction(col: SplitMergeFunction[_])(implicit ctx: TokenizeContext): String = col match {
    case SplitByChar(sep: StringColMagnet[_], col: StringColMagnet[_]) =>
      // Some small optimizations
      val separator = sep.column match {
        case c: Const[_] => c.const.asInstanceOf[String]
      }

      if (separator.length == 1) {
        val s = separator.charAt(0).toInt

        // https://en.wikipedia.org/wiki/List_of_Unicode_characters
        // 34 == DoubleQuote ("), 39 == Single Quote ('),
        // 47 == Forward Slash (/), 92 == Backward Slash (\\)
        // 96 == Grave Accent (` under tilde)
        if (s == 34 || s == 39 || s == 92 || s == 96) {
          s"splitByChar(char($s), ${tokenizeColumn(col.column)})"
        } else if (s >= 32 && s <= 126) {
          s"splitByChar(${tokenizeColumn(sep.column)}, ${tokenizeColumn(col.column)})"
        } else {
          s"splitByChar(char($s), ${tokenizeColumn(col.column)})"
        }
      } else {
        s"splitByString(${tokenizeColumn(sep.column)}, ${tokenizeColumn(col.column)})"
      }
    case SplitByString(sep: StringColMagnet[_], col: StringColMagnet[_]) =>
      s"splitByString(${tokenizeColumn(sep.column)}, ${tokenizeColumn(col.column)})"
    case ArrayStringConcat(col: ArrayColMagnet[_], sep: StringColMagnet[_]) =>
      s"arrayStringConcat(${tokenizeColumn(col.column)}, ${tokenizeColumn(sep.column)})"
    case AlphaTokens(col: StringColMagnet[_]) => s"alphaTokens(${tokenizeColumn(col.column)})"
  }
}
