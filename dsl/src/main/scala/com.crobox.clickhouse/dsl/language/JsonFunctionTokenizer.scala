package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait JsonFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeJsonFunction(col: JsonFunction[_]): String = {
    val command = col match {
      case VisitParamHas(_, _)           => "visitParamHas"
      case VisitParamExtractUInt(_, _)   => "visitParamExtractUInt"
      case VisitParamExtractInt(_, _)    => "visitParamExtractInt"
      case VisitParamExtractFloat(_, _)  => "visitParamExtractFloat"
      case VisitParamExtractBool(_, _)   => "visitParamExtractBool"
      case VisitParamExtractRaw(_, _)    => "visitParamExtractRaw"
      case VisitParamExtractString(_, _) => "visitParamExtractString"
    }

    s"$command(${tokenizeColumn(col.params.column)},${tokenizeColumn(col.fieldName.column)})"
  }

}
