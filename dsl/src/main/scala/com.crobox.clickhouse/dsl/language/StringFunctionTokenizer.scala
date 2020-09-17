package com.crobox.clickhouse.dsl.language

import java.util.UUID

import com.crobox.clickhouse.dsl._

trait StringFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  protected def tokenizeStringCol(col: StringFunctionCol[_])(implicit ctx: TokenizeContext): String =
    col match {
      case Empty(c) =>
        s"empty(${tokenizeColumn(c.column)})"
      case NotEmpty(c) =>
        c.column match {
          case _: TableColumn[UUID] => s"${tokenizeColumn(c.column)} != '0'"
          case _                    => s"notEmpty(${tokenizeColumn(c.column)})"
        }
      case Length(c) =>
        s"length(${tokenizeColumn(c.column)})"
      case LengthUTF8(c) =>
        s"lengthUTF8(${tokenizeColumn(c.column)})"
      case Lower(c) =>
        s"lower(${tokenizeColumn(c.column)})"
      case Upper(c) =>
        s"upper(${tokenizeColumn(c.column)})"
      case LowerUTF8(c) =>
        s"lowerUTF8(${tokenizeColumn(c.column)})"
      case UpperUTF8(c) =>
        s"upperUTF8(${tokenizeColumn(c.column)})"
      case Reverse(c) =>
        s"reverse(${tokenizeColumn(c.column)})"
      case ReverseUTF8(c) =>
        s"reverseUTF8(${tokenizeColumn(c.column)})"
      case Concat(col1, col2, columns @ _*) =>
        s"concat(${tokenizeColumn(col1.column)}, ${tokenizeSeqCol(col2.column, columns.map(_.column): _*)})"
      case Substring(c, offset, length) =>
        s"substring(${tokenizeColumn(c.column)},${tokenizeColumn(offset.column)},${tokenizeColumn(length.column)})"
      case SubstringUTF8(c, offset, length) =>
        s"substringUTF8(${tokenizeColumn(c.column)},${tokenizeColumn(offset.column)},${tokenizeColumn(length.column)})"
      case AppendTrailingCharIfAbsent(c, char) =>
        s"appendTrailingCharIfAbsent(${tokenizeColumn(c.column)},${tokenizeColumn(char.column)})"
      case ConvertCharset(c, from, to) =>
        s"convertCharset(${tokenizeColumn(c.column)},${tokenizeColumn(from.column)},${tokenizeColumn(to.column)})"
    }
}
