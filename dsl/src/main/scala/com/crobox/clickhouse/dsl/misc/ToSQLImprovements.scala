package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.{ClickhouseTokenizerModule, TokenizeContext}

trait ToSQLImprovements extends ClickhouseTokenizerModule {

  def toSql(condition: ExpressionColumn[Boolean])(implicit ctx: TokenizeContext): String = {
    val sql = toSql(select(All()).where(condition).internalQuery)(ctx)
    sql.substring(sql.indexOf("WHERE"), sql.indexOf(" FORMAT"))
  }

  def toSql(condition: Option[ExpressionColumn[Boolean]])(implicit ctx: TokenizeContext): String =
    condition.map(c => toSql(c)(ctx)).getOrElse("")

  def toSql(column: TableColumn[_])(implicit ctx: TokenizeContext): String = tokenizeColumn(column)(ctx)
}

/**
 * Each entry point gets its own `TokenizeContext`. The context accumulates mutable state (join numbering, table
 * aliases) as it tokenizes, so sharing one across unrelated calls would let one query's aliases leak into the next.
 */
object ToSQLImprovements extends ToSQLImprovements {

  def toSql(condition: ExpressionColumn[Boolean]): String = super.toSql(condition)(TokenizeContext())

  def toSql(condition: Option[ExpressionColumn[Boolean]]): String = super.toSql(condition)(TokenizeContext())

  def toSql(column: TableColumn[_]): String = super.toSql(column)(TokenizeContext())
}
