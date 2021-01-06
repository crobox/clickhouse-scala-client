package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait LogicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeLogicalFunction(col: LogicalFunction)(implicit ctx: TokenizeContext): String =
    (col.left.asOption, col.right.asOption) match {
      case (None, None)        => "1"
      case (Some(left), None)  => tokenizeColumn(left)
      case (None, Some(right)) => tokenizeColumn(right)
      case (Some(left), Some(right)) =>
        col.operator match {
          case And =>
            (surroundWithBrackets(left, col.operator), surroundWithBrackets(right, col.operator)) match {
              case ("1", "1")                => "1" // LEFT & RIGHT are true, AND succeeds
              case ("1", rightClause)        => rightClause // LEFT is true, only tokenize RIGHT
              case ("0", _)                  => "0" // LEFT is false, AND fails
              case (leftClause, "1")         => leftClause // RIGHT is true, only tokenize LEFT
              case (_, "0")                  => "0" // RIGHT is false, AND fails
              case (leftClause, rightClause) => s"$leftClause AND $rightClause"
            }
          case Or =>
            (surroundWithBrackets(left, col.operator), surroundWithBrackets(right, col.operator)) match {
              case ("0", "0")                => "0" // LEFT & RIGHT are false, OR fails
              case ("0", rightClause)        => rightClause // LEFT is false, only tokenize RIGHT
              case ("1", _)                  => "1" // LEFT is true, OR succeeds
              case (leftClause, "0")         => leftClause // RIGHT is false, only tokenize LEFT
              case (_, "1")                  => "1" // RIGHT is true, OR succeeds
              case (leftClause, rightClause) => s"$leftClause OR $rightClause"

            }
          case Xor =>
            (surroundWithBrackets(left, col.operator), surroundWithBrackets(right, col.operator)) match {
              case ("0", "0")                => "0" // LEFT & RIGHT are false, XOR fails
              case ("1", "1")                => "0" // LEFT & RIGHT are true, XOR fails
              case ("0", rightClause)        => rightClause // LEFT is false, only tokenize RIGHT
              case (leftClause, "0")         => leftClause // RIGHT is false, only tokenize LEFT
              case ("1", rightClause)        => s"not($rightClause)" // LEFT is true, RIGHT MUST BE FALSE
              case (leftClause, "1")         => s"not($leftClause)" // RIGHT is true, LEFT MUST BE FALSE
              case (leftClause, rightClause) => s"xor($leftClause, $rightClause)"

            }
          case Not => s"not(${tokenizeColumn(left)})"
        }
    }

  private def surroundWithBrackets(col: TableColumn[Boolean], operator: LogicalOperator)(
      implicit ctx: TokenizeContext
  ): String =
    col match {
      case c: LogicalFunction if c.operator == And && operator == Or => surroundWithBrackets(tokenizeColumn(col))
      case c: LogicalFunction if c.operator == Or && operator == And => surroundWithBrackets(tokenizeColumn(col))
      case _                                                         => tokenizeColumn(col)
    }

  private def surroundWithBrackets(evaluated: String)(implicit ctx: TokenizeContext): String =
    // the underlying column c might be a complex logical function that first needs to be evaluated.
    // e.g. or(1 or true or not(a)) ==> not(a)
    // We need to detect if this is a single clause OR a multiple clauses
    if (evaluated.indexOf(" AND ") == -1 && evaluated.indexOf(" OR ") == -1) {
      // we have a single clause
      evaluated
    } else {
      // we have multiple clauses, surround with brackets
      s"($evaluated)"
    }
}
