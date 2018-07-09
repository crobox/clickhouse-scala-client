package com.crobox.clickhouse.stream

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.stream.ClickhouseBulkActor.{ClickhouseIndexingException, Insert}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

object ClickhouseSink extends LazyLogging {

  def insertSink(config: Config, client: ClickhouseClient)(implicit ec: ExecutionContext): Sink[Insert, Future[Done]] = {
    val indexerConfig = config.getConfig("crobox.clickhouse.indexer")
    Flow[Insert]
      .groupBy(Int.MaxValue, _.table)
      .groupedWithin(indexerConfig.getInt("batch-size"), indexerConfig.getDuration("flush-interval").getSeconds seconds)
      .mapAsync(indexerConfig.getInt("concurrent-requests"))(inserts => {
        val table = inserts.head.table
        val insertQuery = s"INSERT INTO $table FORMAT JSONEachRow"
        val payload = inserts.map(_.jsonRow)
        val payloadSql = payload.mkString("\n")
        client.execute(insertQuery, payloadSql) recover {
          case ex =>
            throw ClickhouseIndexingException("failed to index", ex, payload, table)
        } map (_ => inserts)
      })
      .mergeSubstreams
      .toMat(Sink.ignore)(Keep.right)
  }
}
