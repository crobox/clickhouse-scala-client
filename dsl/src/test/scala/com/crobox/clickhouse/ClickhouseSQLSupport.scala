package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.dsl.{InternalQuery, OperationalQuery, TableColumn}

trait ClickhouseSQLSupport {
  this: ClickhouseTokenizerModule =>

  def toSQL(condition: TableColumn[Boolean]): String = toSQL(Option(condition))

  def toSQL(condition: Option[TableColumn[Boolean]]): String = {
    val s = toSql(InternalQuery(where = condition))
    s.substring("WHERE ".length, s.indexOf("FORMAT")).trim
  }

  def toSQL(query: OperationalQuery): String = toSQL(query, stripBeforeWhere = true)

  def toSQL(query: OperationalQuery, stripBeforeWhere: Boolean): String = {
    val sql = toSql(query.internalQuery)
    if (stripBeforeWhere) {
      sql.indexOf("WHERE") match {
        case idx if idx > 0 => sql.substring(idx, sql.indexOf(" FORMAT")).trim
        case _              => sql.substring(0, sql.indexOf(" FORMAT")).trim
      }
    } else sql.substring(0, sql.indexOf(" FORMAT")).trim
  }
}
