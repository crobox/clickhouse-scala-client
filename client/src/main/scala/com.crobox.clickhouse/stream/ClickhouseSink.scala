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

sealed trait TableOperation {
  def table: String

  def jsonRow: String

  def cluster: Option[String]
}

case class Insert(table: String, jsonRow: String) extends TableOperation {
  override def cluster: Option[String] = None
}

case class Optimize(table: String, cluster: Option[String]) extends TableOperation {
  override def jsonRow: String = ""
}

object ClickhouseSink extends LazyLogging {

  def operationSink(config: Config, client: ClickhouseClient, indexerName: Option[String] = None)(
    implicit ec: ExecutionContext,
    settings: QuerySettings = QuerySettings()
  ): Sink[TableOperation, Future[Done]] = {
    val indexerGeneralConfig = config.getConfig("crobox.clickhouse.indexer")
    val mergedIndexerConfig = indexerName
      .flatMap(
        theIndexName =>
          if (indexerGeneralConfig.hasPath(theIndexName))
            Some(indexerGeneralConfig.getConfig(theIndexName).withFallback(indexerGeneralConfig))
          else None
      )
      .getOrElse(indexerGeneralConfig)
    val batchSize = mergedIndexerConfig.getInt("batch-size")
    val flushInterval = mergedIndexerConfig.getDuration("flush-interval").getSeconds.seconds
    Flow[TableOperation]
      .groupBy(Int.MaxValue, _.table)
      .groupedWithin(batchSize, flushInterval)
      .mapAsync(mergedIndexerConfig.getInt("concurrent-requests"))(operations => {
        val table = operations.head.table
        var insertPayload: Seq[String] = Seq.empty
        var optimizeQuery: Option[String] = None

        operations.foreach {
          case o: Insert => insertPayload = insertPayload :+ o.jsonRow
          case o: Optimize => optimizeQuery = Some(buildOptimizeQuery(o.table, o.cluster))
        }

        logger.debug(s"Inserting ${insertPayload.size} entries in table: $table. Group Within: ($batchSize - $flushInterval)")
        insertOp(table, insertPayload, client)
          .flatMap(_ => optimizeQuery match {
            case Some(o) => optimizeOp(table, o, client)
            case _ => Future.successful("")
          })
          .map(_ => operations)
      })
      .mergeSubstreams
      .toMat(Sink.ignore)(Keep.right)
  }

  private def insertOp(table: String, payload: Seq[String], client: ClickhouseClient)(
    implicit ec: ExecutionContext,
    settings: QuerySettings = QuerySettings()
  ): Future[String] = {
    val insertQuery = s"INSERT INTO $table FORMAT JSONEachRow"
    client
      .execute(insertQuery, payload.mkString("\n"))
      .recover {
        case ex => throw ClickhouseIndexingException("failed to index", ex, payload, table)
      }
  }

  private def optimizeOp(table: String, query: String, client: ClickhouseClient)(
    implicit ec: ExecutionContext,
    settings: QuerySettings = QuerySettings()
  ): Future[String] = {
    logger.info(s"Optimizing table $table")
    client.execute(query).recover {
      case ex => throw ClickhouseIndexingException(s"failed to optimize $table", ex, Seq(), table)
    }
  }

  private def buildOptimizeQuery(table: String, cluster: Option[String]): String =
    s"OPTIMIZE TABLE $table${cluster.map(s => s"_local ON CLUSTER $s").getOrElse("")} FINAL"
}
