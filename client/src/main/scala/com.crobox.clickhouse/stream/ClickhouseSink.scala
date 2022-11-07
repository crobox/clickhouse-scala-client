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
}

case class Insert(table: String, jsonRow: String) extends TableOperation

case class Optimize(table: String, localTable: Option[String], cluster: Option[String]) extends TableOperation

object ClickhouseSink extends LazyLogging {

  @deprecated("use [[#toSink()]] instead")
  def insertSink(config: Config, client: ClickhouseClient, indexerName: Option[String] = None)(
      implicit ec: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Sink[Insert, Future[Done]] = toSink(config, client, indexerName)

  def toSink(config: Config, client: ClickhouseClient, indexerName: Option[String] = None)(
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
    val batchSize     = mergedIndexerConfig.getInt("batch-size")
    val flushInterval = mergedIndexerConfig.getDuration("flush-interval").getSeconds.seconds
    Flow[TableOperation]
      .groupBy(Int.MaxValue, _.table)
      .groupedWithin(batchSize, flushInterval)
      .mapAsync(mergedIndexerConfig.getInt("concurrent-requests"))(operations => {
        val table = operations.head.table
        logger.debug(
          s"Executing ${operations.size} operations on table: $table. Group Within: ($batchSize - $flushInterval)"
        )

        // split operations based on their type
        var optimize: Option[Optimize] = None
        val payload = operations.flatMap {
          case op: Insert   => Option(op.jsonRow)
          case op: Optimize => optimize = Option(op); None
        }

        if (payload.nonEmpty) {
          optimize match {
            case Some(statement) => insertTable(client, table, payload).flatMap(_ => optimizeTable(client, statement))
            case _               => insertTable(client, table, payload)
          }
        } else {
          optimize match {
            case Some(statement) => optimizeTable(client, statement)
            case _ =>
              logger.warn(s"No insert or optimize statements for table: $table. How is batch triggered?")
              Future.successful("")
          }
        }
      })
      .mergeSubstreams
      .toMat(Sink.ignore)(Keep.right)
  }

  private def insertTable(client: ClickhouseClient, table: String, payload: Seq[String])(
      implicit ec: ExecutionContext,
      settings: QuerySettings
  ): Future[String] =
    if (payload.nonEmpty) {
      logger.debug(s"Inserting ${payload.size} entries in table: $table.")
      client
        .execute(s"INSERT INTO $table FORMAT JSONEachRow", payload.mkString("\n"))
        .recover {
          case ex => throw ClickhouseIndexingException("failed to index", ex, payload, table)
        }
    } else Future.successful("")

  private def optimizeTable(
      client: ClickhouseClient,
      statement: Optimize
  )(implicit ec: ExecutionContext, settings: QuerySettings): Future[String] = {
    val table = statement.localTable.getOrElse(statement.table)
    logger.debug(s"Optimizing table: $table.")
    client
      .execute(s"OPTIMIZE TABLE $table${statement.cluster.map(s => s" ON CLUSTER $s").getOrElse("")} FINAL")
      .recover {
        case ex => throw ClickhouseIndexingException(s"failed to optimize $table", ex, Seq(), table)
      }
  }
}
