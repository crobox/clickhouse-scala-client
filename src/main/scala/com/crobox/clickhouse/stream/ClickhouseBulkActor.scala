package com.crobox.clickhouse.stream

import akka.actor.{Actor, ActorRef, Cancellable, Props}
import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.stream.ClickhouseBulkActor.ClickhouseIndexingException
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class ClickhouseBulkActor(targetTable: String,
                          client: ClickhouseClient,
                          config: SubscriberConfig,
                          moreRequester: Option[ActorRef] = None)
    extends Actor
    with LazyLogging {

  logger.info(s"Starting ClickhouseBulkActor for $targetTable")

  import context.system

  implicit val ec: ExecutionContext = context.dispatcher

  private val buffer = new ArrayBuffer[String]()
  buffer.sizeHint(config.batchSize)

  private var completed = false

  // total number of documents acknowledged at the elasticsearch cluster level but pending confirmation of index
  private var sent: Long = 0L

  // total number of documents confirmed as successful
  private var confirmed: Long = 0L

  // total number of documents that failed the retry attempts and are ignored
  private var failed: Long = 0L

  // Create a scheduler if a flushInterval is provided. This scheduler will be used to force indexing, otherwise
  // we can be stuck at batchSize-1 waiting for the nth message to arrive.
  //
  // It has been suggested we can use ReceiveTimeout here, but one reason we can't is because BulkResult messages,
  // will cause the timeout period to be reset, but they shouldn't interfere with the flush interval.
  private val flushIntervalScheduler: Option[Cancellable] = config.flushInterval.map { interval =>
    system.scheduler.schedule(interval, interval, self, ClickhouseBulkActor.ForceIndexing)
  }

  // If flushAfter is specified then after each message, a scheduler is created to force indexing if no documents
  // are received within the given duration.
  private var flushAfterScheduler: Option[Cancellable] = None

  private def resetFlushAfterScheduler(): Unit = {
    flushAfterScheduler.foreach(_.cancel)
    flushAfterScheduler = config.flushAfter.map { interval =>
      system.scheduler.scheduleOnce(interval, self, ClickhouseBulkActor.ForceIndexing)
    }
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.warn(s"Restarting bulk actor, indexing buffer with size ${buffer.size}")
    index()
    super.preRestart(reason, message)
  }

  def receive: Receive = {
    case t: Throwable                  => handleError(t)
    case ClickhouseBulkActor.Completed =>
      // since we are completed at the publisher level, we should send all remaining documents because a complete
      // batch cannot happen now
      if (buffer.nonEmpty)
        index()
      completed = true
      shutdownIfAllConfirmed()
    case m: ClickhouseBulkActor.Insert =>
      if (m.table != targetTable) {
        logger.warn(s"Received insert for ${m.table} but this indexer writes to $targetTable")
      }
      buffer.append(m.jsonRow)
      if (buffer.size == config.batchSize) {
        index()
      } else {
        resetFlushAfterScheduler()
      }
    case ClickhouseBulkActor.ForceIndexing =>
      if (buffer.nonEmpty)
        index()

    case ClickhouseBulkActor.FlushSuccess(result, count) =>
      confirmed = confirmed + count
      logger.debug(s"Clickhouse $targetTable confirmed: $confirmed (failed: $failed)")
      config.successCallback(targetTable, count)
      checkCompleteOrRequestNext(count)

    case ClickhouseBulkActor.FlushFailure(ex, count, payload) =>
      failed = failed + count
      logger.error(s"Exception while indexing $count items. failed: $failed, (confirmed: $confirmed)", ex)
      config.failureCallback(
        ClickhouseIndexingException(s"Exception while indexing $count items. failed: $failed, (confirmed: $confirmed)",
                                    ex,
                                    payload,
                                    targetTable)
      )
      checkCompleteOrRequestNext(count)
  }

  // need to check if we're completed, because if we are then this might be the last pending conf
  // and if it is, we can shutdown.
  private def checkCompleteOrRequestNext(n: Int): Unit =
    if (completed) shutdownIfAllConfirmed()
    else moreRequester.getOrElse(self) ! ClickhouseBulkActor.Request(n)

  // Stops the schedulers if they exist
  override def postStop(): Unit = {
    flushIntervalScheduler.map(_.cancel)
    flushAfterScheduler.map(_.cancel)
  }

  private def shutdownIfAllConfirmed(): Unit =
    if (confirmed + failed == sent) {
      context.stop(self)
    }

  private def handleError(t: Throwable): Unit = {

    // if an error we will forward to parent so the subscription can be canceled
    // as we cannot for sure handle further elements
    // and the error may be from outside the subscriber
    moreRequester.foreach(_ ! t)
    logger.error("Error", t)
    buffer.clear()
    context.stop(self)
  }

  private def index(): Unit = {
    val count = buffer.size
    logger.debug(s"Inserting $count")
    sent = sent + count
    logger.debug(s"Clickhouse $targetTable sent: $sent (confirmed: $confirmed, failed: $failed)")
    send(buffer.toList, count)
    buffer.clear()

    // buffer is now empty so no point keeping a scheduled flush after operation
    flushAfterScheduler.foreach(_.cancel)
    flushAfterScheduler = None
  }

  private def send(payload: Seq[String], count: Int): Unit =
    if (payload.nonEmpty) {
      val insertQuery = s"INSERT INTO $targetTable FORMAT JSONEachRow"

      val payloadSql = payload.mkString("\n")

      client.execute(insertQuery, payloadSql) onComplete {
        case Failure(e) =>
          self ! ClickhouseBulkActor.FlushFailure(e, count, payload)
        case Success(resp: String) =>
          self ! ClickhouseBulkActor.FlushSuccess(resp, count)
      }
    }
}

object ClickhouseBulkActor {

  def props(targetTable: String,
            client: ClickhouseClient,
            config: SubscriberConfig,
            moreRequester: Option[ActorRef] = None): Props =
    Props(new ClickhouseBulkActor(targetTable, client, config, moreRequester))

  // signifies that the upstream publisher has completed (NOT that a bulk request has succeeded)
  case object Completed

  case object ForceIndexing

  case class Request(size: Int)

  case class Insert(table: String, jsonRow: String)

  case class Send(req: String, attempts: Int)

  case class FlushSuccess(result: String, count: Int)

  case class FlushFailure(ex: Throwable, count: Int, payload: Seq[String])

  case class ClickhouseIndexingException(msg: String, cause: Throwable, payload: Seq[String], table: String)
      extends RuntimeException(msg, cause)

}
