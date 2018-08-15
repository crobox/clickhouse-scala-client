package com.crobox.clickhouse.dsl
import com.crobox.clickhouse.dsl.StringFunctions.{ConcatString, SplitString}

trait StringFunctions {

  def splitBy(column: TableColumn[String], separator: String): TableColumn[Seq[String]] = {
    require(separator.length > 0, "The separator cannot be empty")
    SplitString(column, separator)
  }

  def mkString(column: TableColumn[Seq[String]], separator: String = ""): TableColumn[String] =
    ConcatString(column, separator)
}

object StringFunctions extends StringFunctions {
  case class SplitString(tableColumn: TableColumn[String], separator: String)
      extends ExpressionColumn[Seq[String]](tableColumn)
  case class ConcatString(tableColumn: TableColumn[Seq[String]], separator: String)
      extends ExpressionColumn[String](tableColumn)

}
