package com.crobox.clickhouse.stream

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.internal.QuerySettings
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

case class ClickhouseIndexingException(msg: String, cause: Throwable, payload: Seq[String], table: String)
    extends RuntimeException(msg, cause)

sealed trait TableOperation {
  def table: String
}

case class Insert(table: String, jsonRow: String) extends TableOperation

case class Optimize(table: String, cluster: Option[String]) extends TableOperation

object ClickhouseSink extends LazyLogging {

  @deprecated("use [[#toSink()]] instead")
  def insertSink(config: Config, client: ClickhouseClient, indexerName: Option[String] = None)(
      implicit ec: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Sink[Insert, Future[Done]] = toSink(config, client, indexerName, sequentialOperationProcessing = true)

  def toSink(config: Config,
             client: ClickhouseClient,
             indexerName: Option[String] = None,
             sequentialOperationProcessing: Boolean = true)(
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
    val batchSize      = mergedIndexerConfig.getInt("batch-size")
    val flushInterval  = mergedIndexerConfig.getDuration("flush-interval").getSeconds.seconds
    val insertBuffer   = mutable.ArrayBuffer.empty[Insert] // keep outside Flow and reuse (memory friendly)
    val optimizeBuffer = mutable.ArrayBuffer.empty[Optimize] // keep outside Flow and reuse (memory friendly)
    Flow[TableOperation]
      .groupBy(Int.MaxValue, _.table)
      .groupedWithin(batchSize, flushInterval)
      .mapAsync(mergedIndexerConfig.getInt("concurrent-requests"))(operations => {
        val table = operations.head.table
        logger.debug(
          s"Executing ${operations.size} operations on table: $table. Group Within: ($batchSize - $flushInterval)"
        )

        // split operations based on their type
        insertBuffer.clear()   // Memory friendly
        optimizeBuffer.clear() // Memory friendly
        operations.foreach {
          case op: Insert   => insertBuffer.append(op)
          case op: Optimize => optimizeBuffer.append(op)
        }
        if (sequentialOperationProcessing) {
          insertOp(insertBuffer, client).flatMap(_ => optimizeOp(optimizeBuffer, client)).map(_ => operations)
        } else {
          Future.sequence(Seq(insertOp(insertBuffer, client), optimizeOp(optimizeBuffer, client))).map(_ => operations)
        }
      })
      .mergeSubstreams
      .toMat(Sink.ignore)(Keep.right)
  }

  private def insertOp(ops: Iterable[Insert], client: ClickhouseClient)(
      implicit ec: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Future[Iterable[String]] =
    Future.sequence(ops.groupBy(_.table).map { t =>
      val payload = t._2.map(_.jsonRow)
      logger.debug(s"Inserting ${payload.size} entries in table: ${t._1}.")
      client
        .execute(s"INSERT INTO ${t._1} FORMAT JSONEachRow", payload.mkString("\n"))
        .recover {
          case ex => throw ClickhouseIndexingException("failed to index", ex, payload.toSeq, t._1)
        }
    })

  private def optimizeOp(ops: Iterable[Optimize], client: ClickhouseClient)(
      implicit ec: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Future[Iterable[String]] = {
    logger.debug(s"Optimizing tables")
    Future.sequence(
      ops.map { o =>
        client
          .execute(s"OPTIMIZE TABLE ${o.table}${o.cluster.map(s => s" ON CLUSTER $s").getOrElse("")} FINAL")
          .recover {
            case ex => throw ClickhouseIndexingException(s"failed to optimize ${o.table}", ex, Seq(), o.table)
          }
      }
    )
  }
}
