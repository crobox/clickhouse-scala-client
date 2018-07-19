package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn}

trait StringFunctions { self: Magnets =>

  abstract class StringFunctionCol[V](val innerCol: AnyTableColumn) extends ExpressionColumn[V](innerCol)

  case class Empty(col: EmptyNonEmptyCol[_])      extends StringFunctionCol[Boolean](col.column)
  case class NotEmpty(col: EmptyNonEmptyCol[_])   extends StringFunctionCol[Boolean](col.column)
  case class Length(col: EmptyNonEmptyCol[_])     extends StringFunctionCol[Int](col.column)
  case class LengthUTF8(col: EmptyNonEmptyCol[_]) extends StringFunctionCol[String](col.column)
  case class Lower(col: StringColMagnet[_])       extends StringFunctionCol[String](col.column)
  case class Upper(col: StringColMagnet[_])       extends StringFunctionCol[String](col.column)
  case class LowerUTF8(col: StringColMagnet[_])   extends StringFunctionCol[String](col.column)
  case class UpperUTF8(col: StringColMagnet[_])   extends StringFunctionCol[String](col.column)
  case class Reverse(col: StringColMagnet[_])     extends StringFunctionCol[String](col.column)
  case class ReverseUTF8(col: StringColMagnet[_]) extends StringFunctionCol[String](col.column)
  case class Concat(col: StringColMagnet[_], col2: StringColMagnet[_], coln: StringColMagnet[_]*)
      extends StringFunctionCol[String](col.column)
  case class Substring(col: StringColMagnet[_], offset: NumericCol[_], length: NumericCol[_])
      extends StringFunctionCol[String](col.column)
  case class SubstringUTF8(col: StringColMagnet[_], offset: NumericCol[_], length: NumericCol[_])
      extends StringFunctionCol[String](col.column)
  case class AppendTrailingCharIfAbsent(col: StringColMagnet[_], c: StringColMagnet[_]) extends StringFunctionCol[String](col.column)
  case class ConvertCharset(col: StringColMagnet[_], from: StringColMagnet[_], to: StringColMagnet[_])
      extends StringFunctionCol[String](col.column)

  // TODO: Enum the charsets?

  def empty(col: EmptyNonEmptyCol[_])      = Empty(col: EmptyNonEmptyCol[_])
  def notEmpty(col: EmptyNonEmptyCol[_])   = NotEmpty(col: EmptyNonEmptyCol[_])
  def length(col: EmptyNonEmptyCol[_])      = Length(col: EmptyNonEmptyCol[_])
  def lengthUTF8(col: EmptyNonEmptyCol[_])  = LengthUTF8(col: EmptyNonEmptyCol[_])
  def lower(col: StringColMagnet[_])       = Lower(col: StringColMagnet[_])
  def upper(col: StringColMagnet[_])       = Upper(col: StringColMagnet[_])
  def lowerUTF8(col: StringColMagnet[_])   = LowerUTF8(col: StringColMagnet[_])
  def upperUTF8(col: StringColMagnet[_])   = UpperUTF8(col: StringColMagnet[_])
  def reverse(col: StringColMagnet[_])     = Reverse(col: StringColMagnet[_])
  def reverseUTF8(col: StringColMagnet[_]) = ReverseUTF8(col: StringColMagnet[_])

  def concat(col: StringColMagnet[_], col2: StringColMagnet[_], coln: StringColMagnet[_]*) =
    Concat(col: StringColMagnet[_], col2: StringColMagnet[_], coln: _*)

  def substring(col: StringColMagnet[_], offset: NumericCol[_], length: NumericCol[_]) =
    Substring(col: StringColMagnet[_], offset: NumericCol[_], length: NumericCol[_])

  def substringUTF8(col: StringColMagnet[_], offset: NumericCol[_], length: NumericCol[_]) =
    SubstringUTF8(col: StringColMagnet[_], offset: NumericCol[_], length: NumericCol[_])

  def appendTrailingCharIfAbsent(col: StringColMagnet[_], c: StringColMagnet[_]) =
    AppendTrailingCharIfAbsent(col: StringColMagnet[_], c: StringColMagnet[_])

  def convertCharset(col: StringColMagnet[_], from: StringColMagnet[_], to: StringColMagnet[_]) =
    ConvertCharset(col: StringColMagnet[_], from: StringColMagnet[_], to: StringColMagnet[_])
}
