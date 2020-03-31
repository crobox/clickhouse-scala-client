package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait LogicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeLogicalFunction(col: LogicalFunction): String =
    (col.left.asOption, col.right.asOption) match {
      case (None, None)        => "1"
      case (Some(left), None)  => tokenizeColumn(left)
      case (None, Some(right)) => tokenizeColumn(right)
      case (Some(left), Some(right)) =>
        col.operator match {
          case And =>
            if (left.isConstTrue)
              tokenizeColumn(right)
            else if (right.isConstTrue)
              tokenizeColumn(left)
            else {
              // Depending on the number of clauses (to the right or left) we should add parentheses/brackets or not
              //s"((${tokenizeColumn(left)}) AND (${tokenizeColumn(right)}))"
              s"${tokenize(left)} AND ${tokenize(right)}"
            }
          case Or =>
            if (left.isConstFalse)
              tokenizeColumn(right)
            else if (right.isConstFalse)
              tokenizeColumn(left)
            else {
              // Depending on the number of clauses (to the right or left) we should add parentheses/brackets or not
              //s"((${tokenizeColumn(left)}) OR (${tokenizeColumn(right)}))"
              s"${tokenize(left)} OR ${tokenize(right)}"
            }
          case Xor =>
            s"xor(${tokenizeColumn(left)}, ${tokenizeColumn(right)})"
          case Not =>
            s"not(${tokenizeColumn(left)})"
        }
    }

  private def tokenize(col: TableColumn[Boolean]): String =
    col match {
      case c: LogicalFunction if c.operator != Not => s"(${tokenizeColumn(c)})"
      case c                                       => tokenizeColumn(c)
    }

}
