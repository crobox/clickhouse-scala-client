package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait LogicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeLogicalFunction(col: LogicalFunction)(implicit ctx: TokenizeContext): String =
    (col.left.asOption, col.right.asOption) match {
      case (None, None)        => "1"
      case (Some(left), None)  => tokenize(left, col.operator)
      case (None, Some(right)) => tokenize(right, col.operator)
      case (Some(left), Some(right)) =>
        col.operator match {
          case And =>
            (tokenize(left, col.operator), tokenize(right, col.operator)) match {
              case ("1", "1")                => "1" // LEFT & RIGHT are true, AND succeeds
              case ("1", rightClause)        => removeBrackets(rightClause) // LEFT is true, only tokenize RIGHT
              case ("0", _)                  => "0" // LEFT is false, AND fails
              case (leftClause, "1")         => removeBrackets(leftClause) // RIGHT is true, only tokenize LEFT
              case (_, "0")                  => "0" // RIGHT is false, AND fails
              case (leftClause, rightClause) => s"$leftClause AND $rightClause"
            }
          case Or =>
            (tokenize(left, col.operator), tokenize(right, col.operator)) match {
              case ("0", "0")                => "0" // LEFT & RIGHT are false, OR fails
              case ("0", rightClause)        => removeBrackets(rightClause) // LEFT is false, only tokenize RIGHT
              case ("1", _)                  => "1" // LEFT is true, OR succeeds
              case (leftClause, "0")         => removeBrackets(leftClause) // RIGHT is false, only tokenize LEFT
              case (_, "1")                  => "1" // RIGHT is true, OR succeeds
              case (leftClause, rightClause) => s"$leftClause OR $rightClause"

            }
          case Xor =>
            (tokenize(left, col.operator), tokenize(right, col.operator)) match {
              case ("0", "0")                => "0" // LEFT & RIGHT are false, XOR fails
              case ("1", "1")                => "0" // LEFT & RIGHT are true, XOR fails
              case ("0", rightClause)        => removeBrackets(rightClause) // LEFT is false, only tokenize RIGHT
              case (leftClause, "0")         => removeBrackets(leftClause) // RIGHT is false, only tokenize LEFT
              case ("1", rightClause)        => s"not(${removeBrackets(rightClause)})" // LEFT is true, RIGHT MUST BE FALSE
              case (leftClause, "1")         => s"not(${removeBrackets(leftClause)})" // RIGHT is true, LEFT MUST BE FALSE
              case (leftClause, rightClause) => s"xor($leftClause, $rightClause)"

            }
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

  private def removeBrackets(clause: String): String =
    if (clause.startsWith("(") && clause.endsWith(")")) clause.substring(1, clause.length - 1) else clause
}
