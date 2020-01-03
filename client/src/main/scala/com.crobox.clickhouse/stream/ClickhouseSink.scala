package com.crobox.clickhouse.stream

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.internal.QuerySettings
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class ClickhouseIndexingException(msg: String, cause: Throwable, payload: Seq[String], table: String)
    extends RuntimeException(msg, cause)
case class Insert(table: String, jsonRow: String)

object ClickhouseSink extends LazyLogging {

  def insertSink(config: Config, client: ClickhouseClient, indexerName: Option[String] = None)(
      implicit ec: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Sink[Insert, Future[Done]] = {
    val indexerGeneralConfig = config.getConfig("crobox.clickhouse.indexer")
    val mergedIndexerConfig = indexerName
      .flatMap(
        theIndexName =>
          if (indexerGeneralConfig.hasPath(theIndexName))
            Some(indexerGeneralConfig.getConfig(theIndexName).withFallback(indexerGeneralConfig))
          else None
      )
      .getOrElse(indexerGeneralConfig)
    Flow[Insert]
      .groupBy(Int.MaxValue, _.table)
      .groupedWithin(mergedIndexerConfig.getInt("batch-size"),
                     mergedIndexerConfig.getDuration("flush-interval").getSeconds.seconds)
      .mapAsyncUnordered(mergedIndexerConfig.getInt("concurrent-requests"))(inserts => {
        val table       = inserts.head.table
        val insertQuery = s"INSERT INTO $table FORMAT JSONEachRow"
        val payload     = inserts.map(_.jsonRow)
        val payloadSql  = payload.mkString("\n")
        client.execute(insertQuery, payloadSql) recover {
          case ex =>
            throw ClickhouseIndexingException("failed to index", ex, payload, table)
        } map (_ => inserts)
      })
      .mergeSubstreams
      .toMat(Sink.ignore)(Keep.right)
  }
}
