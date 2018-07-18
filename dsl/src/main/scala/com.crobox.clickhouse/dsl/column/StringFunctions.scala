package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn}

trait StringFunctions { self: Magnets =>

  abstract class StringFunctionCol[V](val innerCol: AnyTableColumn) extends ExpressionColumn[V](innerCol)

  case class Empty(col: EmptyNonEmptyCol)      extends StringFunctionCol[Boolean](col.column)
  case class NotEmpty(col: EmptyNonEmptyCol)   extends StringFunctionCol[Boolean](col.column)
  case class Length(col: EmptyNonEmptyCol)     extends StringFunctionCol[Int](col.column)
  case class LengthUTF8(col: EmptyNonEmptyCol) extends StringFunctionCol[String](col.column)
  case class Lower(col: StringColMagnet)       extends StringFunctionCol[String](col.column)
  case class Upper(col: StringColMagnet)       extends StringFunctionCol[String](col.column)
  case class LowerUTF8(col: StringColMagnet)   extends StringFunctionCol[String](col.column)
  case class UpperUTF8(col: StringColMagnet)   extends StringFunctionCol[String](col.column)
  case class Reverse(col: StringColMagnet)     extends StringFunctionCol[String](col.column)
  case class ReverseUTF8(col: StringColMagnet) extends StringFunctionCol[String](col.column)
  case class Concat(col: StringColMagnet, col2: StringColMagnet, coln: StringColMagnet*)
      extends StringFunctionCol[String](col.column)
  case class Substring(col: StringColMagnet, offset: NumericCol, length: NumericCol)
      extends StringFunctionCol[String](col.column)
  case class SubstringUTF8(col: StringColMagnet, offset: NumericCol, length: NumericCol)
      extends StringFunctionCol[String](col.column)
  case class AppendTrailingCharIfAbsent(col: StringColMagnet, c: StringColMagnet) extends StringFunctionCol[String](col.column)
  case class ConvertCharset(col: StringColMagnet, from: StringColMagnet, to: StringColMagnet)
      extends StringFunctionCol[String](col.column)

  // TODO: Enum the charsets?

  def empty(col: EmptyNonEmptyCol)      = Empty(col: EmptyNonEmptyCol)
  def notEmpty(col: EmptyNonEmptyCol)   = NotEmpty(col: EmptyNonEmptyCol)
  def length(col: EmptyNonEmptyCol)      = Length(col: EmptyNonEmptyCol)
  def lengthUTF8(col: EmptyNonEmptyCol)  = LengthUTF8(col: EmptyNonEmptyCol)
  def lower(col: StringColMagnet)       = Lower(col: StringColMagnet)
  def upper(col: StringColMagnet)       = Upper(col: StringColMagnet)
  def lowerUTF8(col: StringColMagnet)   = LowerUTF8(col: StringColMagnet)
  def upperUTF8(col: StringColMagnet)   = UpperUTF8(col: StringColMagnet)
  def reverse(col: StringColMagnet)     = Reverse(col: StringColMagnet)
  def reverseUTF8(col: StringColMagnet) = ReverseUTF8(col: StringColMagnet)

  def concat(col: StringColMagnet, col2: StringColMagnet, coln: StringColMagnet*) =
    Concat(col: StringColMagnet, col2: StringColMagnet, coln: _*)

  def substring(col: StringColMagnet, offset: NumericCol, length: NumericCol) =
    Substring(col: StringColMagnet, offset: NumericCol, length: NumericCol)

  def substringUTF8(col: StringColMagnet, offset: NumericCol, length: NumericCol) =
    SubstringUTF8(col: StringColMagnet, offset: NumericCol, length: NumericCol)

  def appendTrailingCharIfAbsent(col: StringColMagnet, c: StringColMagnet) =
    AppendTrailingCharIfAbsent(col: StringColMagnet, c: StringColMagnet)

  def convertCharset(col: StringColMagnet, from: StringColMagnet, to: StringColMagnet) =
    ConvertCharset(col: StringColMagnet, from: StringColMagnet, to: StringColMagnet)
}
