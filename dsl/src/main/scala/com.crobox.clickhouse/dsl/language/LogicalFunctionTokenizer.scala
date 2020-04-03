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
              s"${tokenize(left, Or)} OR ${tokenize(right, Or)}"
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

  private def tokenize(col: TableColumn[Boolean], operator: LogicalOperator): String =
    col match {
      case c: LogicalFunction if c.operator == operator => s"${tokenizeColumn(c)}"
      case c: LogicalFunction if c.operator != Not      => s"(${tokenizeColumn(c)})"
      case c                                            => tokenizeColumn(c)
    }
//    col match {
//      case c: LogicalFunction =>
//        c.operator match {
//          case Not => tokenizeColumn(c)
//          case Or  =>
//            // we need to check if the right / left clause is an OR clause. If so, we can join right/left clauses together
//            val left = c.left match {
//              case left: LogicalFunction if left.operator == Or => tokenize(left)
//              case _                                            => tokenize(left)
//            }
//            val right = c.right match {
//              case right: LogicalFunction if right.operator == Or => tokenize(right)
//              case _                                              => tokenize(right)
//            }
//            s"(${tokenizeColumn(c)})"
//          case _ => s"(${tokenizeColumn(c)})"
//        }
//      case c => tokenizeColumn(c)
}
