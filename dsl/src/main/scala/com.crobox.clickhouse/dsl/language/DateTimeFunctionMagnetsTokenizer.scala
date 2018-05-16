package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.column.DateTimeFunctionsMagnets._
import com.dongxiguo.fastring.Fastring.Implicits._

trait DateTimeFunctionMagnetsTokenizer { self: ClickhouseTokenizerModule =>
  protected def tokenizeDateTimeColumn(col: DateTimeFunction[_]): String = {
    col match {
      case Year(tableColumn: DDTimeMagnet) => fast"toYear(${tokenizeColumn(tableColumn.column)})"
    }
  }

}
