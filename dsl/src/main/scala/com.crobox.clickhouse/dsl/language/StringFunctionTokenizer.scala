package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait StringFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeStringCol(col: StringFunctionCol[_]): String =
    col match {
      case Empty(c)       => fast"empty(${tokenizeColumn(c.column)})"
      case NotEmpty(c)    => fast"notEmpty(${tokenizeColumn(c.column)})"
      case Length(c)      => fast"length(${tokenizeColumn(c.column)})"
      case LengthUTF8(c)  => fast"lengthUTF8(${tokenizeColumn(c)})"
      case Lower(c)       => fast"lower(${tokenizeColumn(c)})"
      case Upper(c)       => fast"upper(${tokenizeColumn(c)})"
      case LowerUTF8(c)   => fast"lowerUTF8(${tokenizeColumn(c)})"
      case UpperUTF8(c)   => fast"upperUTF8(${tokenizeColumn(c)})"
      case Reverse(c)     => fast"reverse(${tokenizeColumn(c)})"
      case ReverseUTF8(c) => fast"reverseUTF8(${tokenizeColumn(c)})"
      case Concat(c, c2, cn@_*) =>
        fast"concat(${tokenizeColumn(c)}, ${tokenizeColumn(c2)}${tokenizeSeqCol(cn)})"
      case Substring(c, offset: Int, length: Int) =>
        fast"substring(${tokenizeColumn(c)},$offset,$length)"
      case SubstringUTF8(c, offset: Int, length: Int)  => fast"substringUTF8(${tokenizeColumn(c)},$offset,$length)"
      case AppendTrailingCharIfAbsent(c, char)         => fast"appendTrailingCharIfAbsent(${tokenizeColumn(c)},$char)"
      case ConvertCharset(c, from: String, to: String) => fast"convertCharset(${tokenizeColumn(c)},$from,$to)"
    }
}
