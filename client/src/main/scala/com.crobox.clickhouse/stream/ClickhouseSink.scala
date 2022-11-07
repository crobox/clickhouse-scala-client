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

case class Optimize(table: String, distributedTable: Option[String], cluster: Option[String]) extends TableOperation

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
        logger.debug(s"Inserting ${payload.size} entries in table: $table.")
        client
          .execute(s"INSERT INTO $table FORMAT JSONEachRow", payload.mkString("\n"))
          .recover {
            case ex => throw ClickhouseIndexingException("failed to index", ex, payload, table)
          }
          .flatMap(result => {
            optimize
              .map(o => {
                val table = o.distributedTable.getOrElse(o.table)
                client
                  .execute(s"OPTIMIZE TABLE $table${o.cluster.map(s => s" ON CLUSTER $s").getOrElse("")} FINAL")
                  .recover {
                    case ex => throw ClickhouseIndexingException(s"failed to optimize $table", ex, Seq(), table)
                  }
              })
              .getOrElse(Future.successful(result))
          })
      })
      .mergeSubstreams
      .toMat(Sink.ignore)(Keep.right)
  }
}
