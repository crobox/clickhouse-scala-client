package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait LogicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  // Flatten nested LogicalFunctions with the same operator to avoid stack overflow
  // Uses iterative approach with a stack to handle deeply nested structures (e.g., 2000+ levels)
  private def flattenLogicalOperator(col: TableColumn[Boolean], operator: LogicalOperator): Seq[TableColumn[Boolean]] = {
    val result = scala.collection.mutable.ArrayBuffer[TableColumn[Boolean]]()
    val stack = scala.collection.mutable.Stack[TableColumn[Boolean]](col)

    while (stack.nonEmpty) {
      stack.pop() match {
        case lf: LogicalFunction if lf.operator == operator =>
          // Push right first so left is processed first (maintains left-to-right order)
          lf.right.asOption.foreach(stack.push)
          lf.left.asOption.foreach(stack.push)
        case other =>
          result += other
      }
    }

    result.toSeq
  }

  def tokenizeLogicalFunction(col: LogicalFunction)(implicit ctx: TokenizeContext): String =
    (col.left.asOption, col.right.asOption) match {
      case (None, None)              => "1"
      case (Some(left), None)        => tokenizeColumn(left)
      case (None, Some(right))       => tokenizeColumn(right)
      case (Some(left), Some(right)) =>
        col.operator match {
          case And =>
            // Flatten nested ANDs to avoid stack overflow
            val flattened = flattenLogicalOperator(left, And) ++ flattenLogicalOperator(right, And)
            val tokenized = flattened.map { c =>
              c match {
                case lf: LogicalFunction if lf.operator == Or => surroundWithBrackets(tokenizeColumn(lf))
                case _ => tokenizeColumn(c)
              }
            }

            // Apply optimization rules
            if (tokenized.contains("0")) "0"  // Any false makes AND fail
            else {
              val filtered = tokenized.filterNot(_ == "1")  // Remove true values
              if (filtered.isEmpty) "1"  // All were true
              else filtered.mkString(" AND ")
            }
          case Or =>
            // Flatten nested ORs to avoid stack overflow
            val flattened = flattenLogicalOperator(left, Or) ++ flattenLogicalOperator(right, Or)
            val tokenized = flattened.map { c =>
              c match {
                case lf: LogicalFunction if lf.operator == And => surroundWithBrackets(tokenizeColumn(lf))
                case _ => tokenizeColumn(c)
              }
            }

            // Apply optimization rules
            if (tokenized.contains("1")) "1"  // Any true makes OR succeed
            else {
              val filtered = tokenized.filterNot(_ == "0")  // Remove false values
              if (filtered.isEmpty) "0"  // All were false
              else filtered.mkString(" OR ")
            }
          case Xor =>
            (surroundWithBrackets(left, col.operator), surroundWithBrackets(right, col.operator)) match {
              case ("0", "0")                => "0"                  // LEFT & RIGHT are false, XOR fails
              case ("1", "1")                => "0"                  // LEFT & RIGHT are true, XOR fails
              case ("0", rightClause)        => rightClause          // LEFT is false, only tokenize RIGHT
              case (leftClause, "0")         => leftClause           // RIGHT is false, only tokenize LEFT
              case ("1", rightClause)        => s"not($rightClause)" // LEFT is true, RIGHT MUST BE FALSE
              case (leftClause, "1")         => s"not($leftClause)"  // RIGHT is true, LEFT MUST BE FALSE
              case (leftClause, rightClause) => s"xor($leftClause, $rightClause)"

            }
          case Not => s"not(${tokenizeColumn(left)})"
        }
    }

  private def surroundWithBrackets(col: TableColumn[Boolean], operator: LogicalOperator)(implicit
      ctx: TokenizeContext
  ): String =
    col match {
      case c: LogicalFunction if c.operator == And && operator == Or => surroundWithBrackets(tokenizeColumn(col))
      case c: LogicalFunction if c.operator == Or && operator == And => surroundWithBrackets(tokenizeColumn(col))
      case c: LogicalFunction if c.operator == operator => tokenizeLogicalFunction(c)
      case _                                                         => tokenizeColumn(col)
    }

  private def surroundWithBrackets(evaluated: String): String =
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
