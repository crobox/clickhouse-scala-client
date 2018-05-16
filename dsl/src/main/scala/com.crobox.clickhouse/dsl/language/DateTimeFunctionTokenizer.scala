package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.column.DateTimeFunctions._
import com.dongxiguo.fastring.Fastring.Implicits._

trait DateTimeFunctionTokenizer { self: ClickhouseTokenizerModule =>
  protected def tokenizeDateTimeColumn(col: DateTimeFunction[_,_]): String = {
    col match {
      case Year(tableColumn: AnyTableColumn) => fast"toYear(${tokenizeColumn(tableColumn)})"
    }
  }

}
