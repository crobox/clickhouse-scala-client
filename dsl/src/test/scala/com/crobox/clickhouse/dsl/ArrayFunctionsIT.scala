package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.dsl.execution.ClickhouseQueryExecutor
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule
import com.crobox.clickhouse.internal.QuerySettings
import com.crobox.clickhouse.testkit.ClickhouseSpec
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Future

class ArrayFunctionsIT extends ClickhouseClientSpec with ClickhouseSpec {
  implicit lazy val chExecutor: ClickhouseQueryExecutor            = ClickhouseQueryExecutor.default(clickClient)
  implicit lazy val clickhouseTokenizer: ClickhouseTokenizerModule = new ClickhouseTokenizerModule {}

  override implicit def patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  private def execute(query: Query): Future[String] = {
    implicit val settings: QuerySettings = QuerySettings()
    clickClient.query(clickhouseTokenizer.toSql(query.internalQuery, None)).map(_.trim)
  }

  it should "arrayFunction: has" in {
    execute(select(has(Array(1, 2, 3, 4), 2))).futureValue.toInt should be(1)
  }

  it should "arrayFunction: hasAny" in {
    execute(select(hasAny(Array(1, 2, 3, 4), Array(2)))).futureValue.toInt should be(1)
    execute(select(hasAny(Array(1, 2, 3, 4), Array(5)))).futureValue.toInt should be(0)
    execute(select(hasAny(Array(1, 2, 3, 4), Array(1,2)))).futureValue.toInt should be(1)
    execute(select(hasAny(Array(1, 2, 3, 4), Array(1,5)))).futureValue.toInt should be(1)
  }

  it should "arrayFunction: hasAll" in {
    execute(select(hasAll(Array(1, 2, 3, 4), Array(1,2)))).futureValue.toInt should be(1)
    execute(select(hasAll(Array(1, 2, 3, 4), Array(1,5)))).futureValue.toInt should be(0)
  }
}
