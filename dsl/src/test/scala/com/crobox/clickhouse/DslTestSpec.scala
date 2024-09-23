package com.crobox.clickhouse

import com.crobox.clickhouse.dsl.language.{ClickhouseTokenizerModule, TokenizeContext}
import com.crobox.clickhouse.dsl.{InternalQuery, OperationalQuery, TableColumn}
import com.crobox.clickhouse.testkit.ClickhouseMatchers
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

trait DslTestSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers
    with TestSchema
    with ClickhouseMatchers
    with ClickhouseTokenizerModule {

  val config: Config                         = ConfigFactory.load()
  val serverVersion: ClickhouseServerVersion = ClickhouseServerVersion(Seq(22, 3))
  override val database: String              = "test"

  implicit def ctx: TokenizeContext = TokenizeContext(serverVersion)

  def toSQL(condition: TableColumn[Boolean]): String = toSQL(Option(condition))

  def toSQL(condition: Option[TableColumn[Boolean]]): String = {
    val s = toSql(InternalQuery(where = condition))(ctx)
    s.substring("WHERE ".length, s.indexOf("FORMAT")).trim
  }

  def toSQL(query: OperationalQuery): String = toSQL(query, stripBeforeWhere = true)

  def toSQL(query: OperationalQuery, stripBeforeWhere: Boolean): String = {
    val sql = toSql(query.internalQuery)(ctx)
    if (stripBeforeWhere) {
      sql.indexOf("WHERE") match {
        case idx if idx > 0 => sql.substring(idx, sql.indexOf(" FORMAT")).trim
        case _              => sql.substring(0, sql.indexOf(" FORMAT")).trim
      }
    } else sql.substring(0, sql.indexOf(" FORMAT")).trim
  }

  def shouldMatch(query: OperationalQuery, expected: String): Assertion = {
    toSql(query.internalQuery, None) should matchSQL(expected)
  }
}
