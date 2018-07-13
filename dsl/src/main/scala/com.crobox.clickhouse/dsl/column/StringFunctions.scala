package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{ExpressionColumn, TableColumn}

trait StringFunctions { self: Magnets =>

  abstract class StringFunctionCol[V](val innerCol: EmptyNonEmptyCol) extends ExpressionColumn[V](innerCol.column)

  case class Empty(col: EmptyNonEmptyCol)                                extends StringFunctionCol[Boolean](col)
  case class NotEmpty(col: EmptyNonEmptyCol)                             extends StringFunctionCol[Boolean](col)
  case class Length(col: EmptyNonEmptyCol)                               extends StringFunctionCol[Int](col)
  case class LengthUTF8[C <: TableColumn[String]](col: TableColumn[String])                extends StringFunctionCol[String](col)
  case class Lower[C <: TableColumn[String]](col: TableColumn[String])                     extends StringFunctionCol[String](col)
  case class Upper[C <: TableColumn[String]](col: TableColumn[String])                     extends StringFunctionCol[String](col)
  case class LowerUTF8[C <: TableColumn[String]](col: TableColumn[String])                 extends StringFunctionCol[String](col)
  case class UpperUTF8[C <: TableColumn[String]](col: TableColumn[String])                 extends StringFunctionCol[String](col)
  case class Reverse[C <: TableColumn[String]](col: TableColumn[String])                   extends StringFunctionCol[String](col)
  case class ReverseUTF8[C <: TableColumn[String]](col: TableColumn[String])               extends StringFunctionCol[String](col)
  case class Concat[C <: TableColumn[String]](col: TableColumn[String], col2: TableColumn[String], coln: TableColumn[String]*) extends StringFunctionCol[String](col)
  case class Substring[C <: TableColumn[String]](col: TableColumn[String], offset: Int, length: Int)
      extends StringFunctionCol[String](col)
  case class SubstringUTF8[C <: TableColumn[String]](col: TableColumn[String], offset: Int, length: Int)
      extends StringFunctionCol[String](col)
  case class AppendTrailingCharIfAbsent[C <: TableColumn[String]](col: TableColumn[String], c: Char)
      extends StringFunctionCol[String](col)
  case class ConvertCharset[C <: TableColumn[String]](col: TableColumn[String], from: String, to: String)
      extends StringFunctionCol[String](col)

  // TODO: Enum the charsets?
  // TODO: MAgnetize

  def empty(col: EmptyNonEmptyCol)                        = Empty(col: EmptyNonEmptyCol)
  def notEmpty(col: EmptyNonEmptyCol)                     = NotEmpty(col: EmptyNonEmptyCol)
  def length(col: TableColumn[String])                    = Length(col: TableColumn[String])
  def lengthUTF8(col: TableColumn[String])                = LengthUTF8(col: TableColumn[String])
  def lower(col: TableColumn[String])                     = Lower(col: TableColumn[String])
  def upper(col: TableColumn[String])                     = Upper(col: TableColumn[String])
  def lowerUTF8(col: TableColumn[String])                 = LowerUTF8(col: TableColumn[String])
  def upperUTF8(col: TableColumn[String])                 = UpperUTF8(col: TableColumn[String])
  def reverse(col: TableColumn[String])                   = Reverse(col: TableColumn[String])
  def reverseUTF8(col: TableColumn[String])               = ReverseUTF8(col: TableColumn[String])
  def concat(col: TableColumn[String], col2: TableColumn[String], coln: TableColumn[String]*) = Concat(col: TableColumn[String], col2: TableColumn[String], coln: _*)
  def substring(col: TableColumn[String], offset: Int, length: Int) = Substring(col: TableColumn[String], offset: Int, length: Int)
  def substringUTF8(col: TableColumn[String], offset: Int, length: Int) = SubstringUTF8(col: TableColumn[String], offset: Int, length: Int)
  def appendTrailingCharIfAbsent(col: TableColumn[String], c: Char) = AppendTrailingCharIfAbsent(col: TableColumn[String], c: Char)
  def convertCharset(col: TableColumn[String], from: String, to: String) = ConvertCharset(col: TableColumn[String], from: String, to: String)
}
