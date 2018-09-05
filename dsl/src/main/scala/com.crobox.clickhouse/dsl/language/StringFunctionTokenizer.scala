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
      case LengthUTF8(c)  => fast"lengthUTF8(${tokenizeColumn(c.column)})"
      case Lower(c)       => fast"lower(${tokenizeColumn(c.column)})"
      case Upper(c)       => fast"upper(${tokenizeColumn(c.column)})"
      case LowerUTF8(c)   => fast"lowerUTF8(${tokenizeColumn(c.column)})"
      case UpperUTF8(c)   => fast"upperUTF8(${tokenizeColumn(c.column)})"
      case Reverse(c)     => fast"reverse(${tokenizeColumn(c.column)})"
      case ReverseUTF8(c) => fast"reverseUTF8(${tokenizeColumn(c.column)})"
      case Concat(c, c2, cn@_*) =>
        fast"concat(${tokenizeColumn(c.column)}, ${tokenizeColumn(c2.column)}${tokenizeSeqCol(cn.map(_.column))})"
      case Substring(c, offset, length) =>
        fast"substring(${tokenizeColumn(c.column)},${tokenizeColumn(offset.column)},${tokenizeColumn(length.column)})"
      case SubstringUTF8(c, offset, length)  => fast"substringUTF8(${tokenizeColumn(c.column)},${tokenizeColumn(offset.column)},${tokenizeColumn(length.column)})"
      case AppendTrailingCharIfAbsent(c, char)         => fast"appendTrailingCharIfAbsent(${tokenizeColumn(c.column)},${tokenizeColumn(char.column)})"
      case ConvertCharset(c, from, to) => fast"convertCharset(${tokenizeColumn(c.column)},${tokenizeColumn(from.column)},${tokenizeColumn(to.column)})"
    }
}
