package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait LogicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeLogicalFunction(col: LogicalFunction)(implicit ctx: TokenizeContext): String =
    (col.left.asOption, col.right.asOption) match {
      case (None, None) => "1"
      case (Some(left), None) =>
        if (left.isConstFalse) "0"
        else if (left.isConstTrue) "1"
        else tokenize(left, col.operator)
      case (None, Some(right)) =>
        if (right.isConstFalse) "0"
        else if (right.isConstTrue) "1"
        else tokenize(right, col.operator)
      case (Some(left), Some(right)) =>
        col.operator match {
          case And =>
            if (left.isConstFalse) "0" // LEFT is false, no need to process RIGHT
            else if (left.isConstTrue) tokenizeColumn(right) // LEFT is true, only tokenize RIGHT
            else if (right.isConstFalse) "0" // RIGHT is false, no need to process LEFT
            else if (right.isConstTrue) tokenizeColumn(left) // RIGHT is true, only tokenize LEFT
            else s"${tokenize(left, col.operator)} AND ${tokenize(right, col.operator)}"
          case Or =>
            if (left.isConstFalse) tokenizeColumn(right) // LEFT is false, only tokenize RIGHT
            else if (left.isConstTrue) "1" // LEFT is true, skip RIGHT
            else if (right.isConstFalse) tokenizeColumn(left) // RIGHT is false, only tokenize LEFT
            else if (right.isConstTrue) "1" // RIGHT is true, skip LEFT
            else s"${tokenize(left, col.operator)} OR ${tokenize(right, col.operator)}"
          case Xor => s"xor(${tokenize(left, col.operator)}, ${tokenize(right, col.operator)})"
          case Not => s"not(${tokenize(left, col.operator)})"
        }
    }

  private def tokenize(col: TableColumn[Boolean],
                       parentOperator: LogicalOperator)(implicit ctx: TokenizeContext): String =
    col match {
      case c: LogicalFunction if c.operator == And && parentOperator == Or => surroundWithBrackets(c)
      case c: LogicalFunction if c.operator == Or && parentOperator == And => surroundWithBrackets(c)
      case c                                                               => tokenizeColumn(c)
    }

  private def surroundWithBrackets(col: LogicalFunction)(implicit ctx: TokenizeContext): String = {
    // the underlying column c might be a complex logical function that first needs to be evaluated.
    // e.g. or(1 or true or not(a)) ==> not(a)
    // We need to detect if this is a single clause OR a multiple clauses
    val evaluated = tokenizeColumn(col)
    if (evaluated.indexOf(" AND ") == -1 && evaluated.indexOf(" OR ") == -1) {
      // we have a single clause
      evaluated
    } else {
      // we have multiple clauses, surround with brackets
      s"($evaluated)"
    }
  }
}
