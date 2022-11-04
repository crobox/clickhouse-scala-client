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

case class Optimize(table: String, cluster: Option[String]) extends TableOperation

object ClickhouseSink extends LazyLogging {

  /**
   * @deprecated added support to optimize table operations <br/>
   *             use [[#toSink()]] instead.
   */
  @deprecated
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
    val batchSize     = mergedIndexerConfig.getInt("batch-size")
    val flushInterval = mergedIndexerConfig.getDuration("flush-interval").getSeconds.seconds
    Flow[Insert]
      .groupBy(Int.MaxValue, _.table)
      .groupedWithin(batchSize, flushInterval)
      .mapAsyncUnordered(mergedIndexerConfig.getInt("concurrent-requests"))(inserts => {
        val table       = inserts.head.table
        val insertQuery = s"INSERT INTO $table FORMAT JSONEachRow"
        val payload     = inserts.map(_.jsonRow)
        logger.debug(s"Inserting ${inserts.size} entries in table: $table. Group Within: ($batchSize - $flushInterval)")
        client
          .execute(insertQuery, payload.mkString("\n"))
          .recover {
            case ex => throw ClickhouseIndexingException("failed to index", ex, payload, table)
          }
          .map(_ => inserts)
      })
      .mergeSubstreams
      .toMat(Sink.ignore)(Keep.right)
  }

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

        insertOp(operations.collect { case o: Insert                     => o }, client)
          .flatMap(_ => optimizeOp(operations.collect { case o: Optimize => o }, client))
          .map(_ => operations)
      })
      .mergeSubstreams
      .toMat(Sink.ignore)(Keep.right)
  }

  private def insertOp(ops: Seq[Insert], client: ClickhouseClient)(
      implicit ec: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Future[Iterable[String]] =
    Future.sequence(ops.groupBy(_.table).map { t =>
      val insertQuery = s"INSERT INTO ${t._1} FORMAT JSONEachRow"
      val payload     = t._2.map(_.jsonRow)
      logger.info(s"Inserting ${payload.size} entries in table: ${t._1}.")
      client
        .execute(insertQuery, payload.mkString("\n"))
        .recover {
          case ex => throw ClickhouseIndexingException("failed to index", ex, payload, t._1)
        }
    })

  private def optimizeOp(ops: Seq[Optimize], client: ClickhouseClient)(
      implicit ec: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Future[Iterable[String]] = {
    logger.info(s"Optimizing tables")
    Future.sequence(
      ops.map { o =>
        val query = buildOptimizeQuery(o.table, o.cluster)
        client.execute(query).recover {
          case ex => throw ClickhouseIndexingException(s"failed to optimize ${o.table}", ex, Seq(), o.table)
        }
      }
    )
  }

  private def buildOptimizeQuery(table: String, cluster: Option[String]): String =
    s"OPTIMIZE TABLE $table${cluster.map(s => s"_local ON CLUSTER $s").getOrElse("")} FINAL"
}
