package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.ClickhouseServerVersion
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.{ClickhouseTokenizerModule, TokenizeContext}

trait ToSQLImprovements extends ClickhouseTokenizerModule {
  implicit def ctx: TokenizeContext = TokenizeContext(ClickhouseServerVersion(versions = Seq(22, 3)))

  def toSql(condition: ExpressionColumn[Boolean])(implicit ctx: TokenizeContext): String = {
    val sql = toSql(select(All()).where(condition).internalQuery)(ctx)
    sql.substring(sql.indexOf("WHERE"), sql.indexOf(" FORMAT"))
  }

  def toSql(condition: Option[ExpressionColumn[Boolean]])(implicit ctx: TokenizeContext): String =
    condition.map(c => toSql(c)(ctx)).getOrElse("")

  def toSql(column: TableColumn[_])(implicit ctx: TokenizeContext): String = tokenizeColumn(column)(ctx)
}

object ToSQLImprovements extends ToSQLImprovements {
  def toSql(condition: ExpressionColumn[Boolean]): String = super.toSql(condition)(ctx)

  def toSql(condition: Option[ExpressionColumn[Boolean]]): String = super.toSql(condition)(ctx)

  def toSql(column: TableColumn[_]): String = super.toSql(column)(ctx)
}
