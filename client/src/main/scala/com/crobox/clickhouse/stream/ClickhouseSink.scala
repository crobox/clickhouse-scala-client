package com.crobox.clickhouse.stream

import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}
import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.internal.QuerySettings
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import com.crobox.clickhouse.internal.ConfigDuration

case class ClickhouseIndexingException(msg: String, cause: Throwable, payload: Seq[String], table: String)
    extends RuntimeException(msg, cause)

sealed trait TableOperation {
  def table: String
}

case class Insert(table: String, jsonRow: String) extends TableOperation

case class Optimize(
    table: String,
    localTable: Option[String] = None,
    cluster: Option[String] = None,
    partition: Option[String] = None,
    `final`: Boolean = true,
    deduplicate: Option[String] = None
) extends TableOperation {

  def toSql: String = {
    var sql = s"OPTIMIZE TABLE ${localTable.getOrElse(table)}"
    cluster.foreach(cluster => sql += s" ON CLUSTER $cluster")
    partition.foreach(partition => sql += s" PARTITION $partition")
    if (`final`) sql += " FINAL"
    deduplicate.foreach(exp => sql += " DEDUPLICATE" + (if (exp.trim.isEmpty) "" else " BY " + exp))
    sql
  }
}

object ClickhouseSink extends LazyLogging {

  def toSink(config: Config, client: ClickhouseClient, indexerName: Option[String] = None)(implicit
      ec: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Sink[TableOperation, Future[Done]] = {
    val mergedIndexerConfig = indexerConfig(config, indexerName)
    val batchSize           = mergedIndexerConfig.getInt("batch-size")
    val flushInterval       = ConfigDuration(mergedIndexerConfig, "flush-interval")
    Flow[TableOperation]
      .groupBy(Int.MaxValue, _.table)
      .groupedWithin(batchSize, flushInterval)
      .mapAsync(mergedIndexerConfig.getInt("concurrent-requests")) { operations =>
        val table = operations.head.table
        logger.debug(
          s"Executing ${operations.size} operations on table: $table. Group Within: ($batchSize - $flushInterval)"
        )

        // split operations based on their type
        var optimize: Option[Optimize] = None
        val payload                    = operations.flatMap {
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
            case _               =>
              logger.warn(s"No insert or optimize statements for table: $table")
              Future.successful("")
          }
        }
      }
      .mergeSubstreams
      .toMat(Sink.ignore)(Keep.right)
  }

  /**
   * Inserts each element's rows, then emits the element's context: a stream can acknowledge its input once the rows are
   * in ClickHouse, for example commit Kafka offsets, instead of when it reads them.
   *
   * Elements are batched up to `batch-size` rows or `flush-interval`, as in [[toSink]], and a batch inserts its rows
   * with one insert per table. Up to `concurrent-requests` batches run at once, and their contexts are emitted in input
   * order. An element without rows still waits for its batch. A failed insert fails the batch with a
   * [[ClickhouseIndexingException]], and none of the batch's contexts are emitted.
   */
  def toFlow[C](config: Config, client: ClickhouseClient, indexerName: Option[String] = None)(implicit
      ec: ExecutionContext,
      settings: QuerySettings = QuerySettings()
  ): Flow[(Seq[Insert], C), C, NotUsed] = {
    val mergedIndexerConfig = indexerConfig(config, indexerName)
    Flow[(Seq[Insert], C)]
      .groupedWeightedWithin(
        mergedIndexerConfig.getInt("batch-size").toLong,
        ConfigDuration(mergedIndexerConfig, "flush-interval")
      )(element => math.max(element._1.size, 1).toLong)
      .mapAsync(mergedIndexerConfig.getInt("concurrent-requests")) { batch =>
        Future
          .traverse(batch.flatMap(_._1).groupBy(_.table).toSeq) { case (table, inserts) =>
            insertTable(client, table, inserts.map(_.jsonRow))
          }
          .map(_ => batch.map(_._2))
      }
      .mapConcat(identity)
  }

  private def indexerConfig(config: Config, indexerName: Option[String]): Config = {
    val indexerGeneralConfig = config.getConfig("crobox.clickhouse.indexer")
    indexerName
      .filter(indexerGeneralConfig.hasPath)
      .map(indexerGeneralConfig.getConfig(_).withFallback(indexerGeneralConfig))
      .getOrElse(indexerGeneralConfig)
  }

  private def insertTable(client: ClickhouseClient, table: String, payload: Seq[String])(implicit
      ec: ExecutionContext,
      settings: QuerySettings
  ): Future[String] = {
    logger.debug(s"Inserting ${payload.size} entries in table: $table.")
    client
      .execute(s"INSERT INTO $table FORMAT JSONEachRow", payload.mkString("\n"))
      .recover { case ex =>
        throw ClickhouseIndexingException("failed to index", ex, payload, table)
      }
  }

  protected[stream] def optimizeTable(
      client: ClickhouseClient,
      statement: Optimize
  )(implicit ec: ExecutionContext, settings: QuerySettings): Future[String] =
    client
      .execute(statement.toSql)
      .recover { case ex =>
        throw ClickhouseIndexingException(
          s"failed to optimize ${statement.table}",
          ex,
          Seq(statement.toSql),
          statement.table
        )
      }
}
