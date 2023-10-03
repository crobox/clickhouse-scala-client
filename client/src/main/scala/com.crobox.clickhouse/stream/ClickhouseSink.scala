package com.crobox.clickhouse.stream

import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink}
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

case class Optimize(table: String,
                    localTable: Option[String] = None,
                    cluster: Option[String] = None,
                    partition: Option[String] = None,
                    `final`: Boolean = true,
                    deduplicate: Option[String] = None)
    extends TableOperation {

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
              logger.warn(s"No insert or optimize statements for table: $table")
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
  ): Future[String] = {
    logger.debug(s"Inserting ${payload.size} entries in table: $table.")
    client
      .execute(s"INSERT INTO $table FORMAT JSONEachRow", payload.mkString("\n"))
      .recover {
        case ex => throw ClickhouseIndexingException("failed to index", ex, payload, table)
      }
  }

  protected[stream] def optimizeTable(
      client: ClickhouseClient,
      statement: Optimize
  )(implicit ec: ExecutionContext, settings: QuerySettings): Future[String] =
    client
      .execute(statement.toSql)
      .recover {
        case ex =>
          throw ClickhouseIndexingException(s"failed to optimize ${statement.table}",
                                            ex,
                                            Seq(statement.toSql),
                                            statement.table)
      }
}
