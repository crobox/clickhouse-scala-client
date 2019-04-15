package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.dongxiguo.fastring.Fastring.Implicits._

trait LogicalFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeLogicalFunction(col: LogicalFunction)(implicit database: Database): String = {
    (col.left.asOption, col.right.asOption) match {
      case (None, None) => "1"
      case (Some(left), None) => tokenizeColumn(left)
      case (None, Some(right)) => tokenizeColumn(right)
      case (Some(left), Some(right)) => col.operator match {
        case And =>
          if (left.isConstTrue)
            tokenizeColumn(right)
          else if (right.isConstTrue)
            tokenizeColumn(left)
          else
            fast"${tokenizeColumn(left)} AND ${tokenizeColumn(right)}"
        case Or =>
          if (left.isConstFalse)
            tokenizeColumn(right)
          else if (right.isConstFalse)
            tokenizeColumn(left)
          else
            fast"((${tokenizeColumn(left)}) OR (${tokenizeColumn(right)}))"
        case Xor =>
          fast"xor(${tokenizeColumn(left)}, ${tokenizeColumn(right)})"
        case Not =>
          fast"not(${tokenizeColumn(left)})"
      }
    }
  }

}
