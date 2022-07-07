package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.dsl.Query
import com.crobox.clickhouse.dsl.execution.{ClickhouseQueryExecutor, QueryResult}
import com.crobox.clickhouse.dsl.language.{ClickhouseTokenizerModule, TokenizeContext}
import com.typesafe.scalalogging.LazyLogging
import spray.json.JsonReader

import scala.concurrent.{ExecutionContext, Future}

object QueryImprovements extends LazyLogging {
  val tokenizer: ClickhouseTokenizerModule = new ClickhouseTokenizerModule {}

  implicit class QueryImpr(query: Query) {

    def executeWithLogging[V: JsonReader](debug: Boolean)(
        implicit executionContext: ExecutionContext,
        clickhouseExecutor: ClickhouseQueryExecutor
    ): Future[QueryResult[V]] = {
      if (debug)
        logger.info(
          s"SQL: ${tokenizer.toSql(query.internalQuery)(TokenizeContext(clickhouseExecutor.client.serverVersion))}"
        )
      clickhouseExecutor.execute(query)
    }

    def executeWithLogging[V: JsonReader](traceId: String)(
        implicit executionContext: ExecutionContext,
        clickhouseExecutor: ClickhouseQueryExecutor
    ): Future[QueryResult[V]] = {
      logger.info(
        s"[$traceId] ${tokenizer.toSql(query.internalQuery)(TokenizeContext(clickhouseExecutor.client.serverVersion))}"
      )
      clickhouseExecutor.execute(query)
    }

    def executeWithLogging[V: JsonReader](
        implicit executionContext: ExecutionContext,
        clickhouseExecutor: ClickhouseQueryExecutor
    ): Future[QueryResult[V]] = {
      logger.info(
        s"SQL: ${tokenizer.toSql(query.internalQuery)(TokenizeContext(clickhouseExecutor.client.serverVersion))}"
      )
      clickhouseExecutor.execute(query)
    }
  }
}
