package com.crobox.clickhouse.stream

import akka.Done
import akka.actor.{Actor, ActorRef, ActorRefFactory, Cancellable, PoisonPill, Props, Terminated}
import com.crobox.clickhouse.ClickhouseClient
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.joda.JodaModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.{Subscriber, Subscription}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}


object ClickhouseIndexingSubscriber extends LazyLogging {

  /**
    * @param client - clickhouse client
    * @param flushInterval - how often to flush(recommended no more than once per second)
    * @param failureCallback - called on every INSERT batch failure with info about (table, batchSize)
    * @param successCallback - called on every INSERT batch success with info about (table, batchSize)
   */
  def default(client: ClickhouseClient, flushInterval: Option[FiniteDuration],
              failureCallback: (String, Long) => Unit = (table, count) => (),
              successCallback: (String, Long) => Unit = (table, count) => ()
             )
             (implicit actorRefFactory: ActorRefFactory): (Subscriber[ClickhouseBulkActor.Insert], Future[Done]) = {
    val completed = Promise[Done]
    val clickSubscriber = new ClickhouseIndexingSubscriber(client, SubscriberConfig(
      batchSize = 64 * 1024,
      errorFn = e => {
        logger.error(e.getMessage, e)
      },
      completionFn = () => {
        logger.debug(s"Completed!")
        completed.success(Done)
      },
      failureCallback = (table, count) => {
        failureCallback(table, count)
      },
      successCallback = (table, count) => {
        failureCallback(table, count)
      },
      flushInterval = flushInterval
    ))
    (clickSubscriber, completed.future)
  }
}

/**
  * @author Sjoerd Mulder
  * @since 16-9-16
  */
class ClickhouseIndexingSubscriber(client: ClickhouseClient,
                                   config: SubscriberConfig)
                                  (implicit actorRefFactory: ActorRefFactory)
  extends Subscriber[ClickhouseBulkActor.Insert] {

    private var actor: ActorRef = _

    override def onSubscribe(sub: Subscription): Unit = {
      // rule 1.9 https://github.com/reactive-streams/reactive-streams-jvm#2.5
      // when the provided Subscriber is null in which case it MUST throw a java.lang.NullPointerException to the caller
      if (sub == null) throw new NullPointerException()
      if (actor == null) {
        actor = actorRefFactory.actorOf(Props(new ClickhouseBulkActorManager(client, sub, config)))
      } else {
        // rule 2.5, must cancel subscription if onSubscribe has been invoked twice
        // https://github.com/reactive-streams/reactive-streams-jvm#2.5
        sub.cancel()
      }
    }

    override def onNext(t: ClickhouseBulkActor.Insert): Unit = {
      if (t == null) throw new NullPointerException("On next should not be called until onSubscribe has returned")
      actor ! t
    }

    override def onError(t: Throwable): Unit = {
      if (t == null) throw new NullPointerException()
      actor ! t
    }

    override def onComplete(): Unit = {
      actor ! ClickhouseBulkActor.Completed
    }

    def close(): Unit = {
      actor ! PoisonPill
    }
  }

class ClickhouseBulkActorManager(client: ClickhouseClient,
                                 subscription: Subscription,
                                 config: SubscriberConfig) extends Actor with LazyLogging {

  private def getActor(table: String): ActorRef = {
    context.child(table).getOrElse({
      val child = context.actorOf(ClickhouseBulkActor.props(table, client, config, Some(self)), table)
      context.watch(child)
      child
    })
  }

  // total number of documents requested from our publisher
  private var requested: Long = 0L

  // requests our initial starting batches, we can request them all at once, and then just request a new batch
  // each time we complete a batch
  override def preStart(): Unit = {
    self ! ClickhouseBulkActor.Request(config.batchSize * config.concurrentRequests)
  }

  override def receive: Receive = {
    case ClickhouseBulkActor.Request(n) =>
      subscription.request(n)
      requested = requested + n
    case t: Throwable =>
      logger.error("Received error for upstream... canceling subscription and stopping", t)
      subscription.cancel()
      shutdown(true)
    case t: ClickhouseBulkActor.Insert =>
      getActor(t.table) ! t
    case Terminated(_) =>
      shutdown()
    case ClickhouseBulkActor.Completed =>
      shutdown(true)
  }

  private def shutdown(downChildren: Boolean = false) = {
    if (context.children.isEmpty) {
      context.stop(self)
    } else if (downChildren) {
      context.children.foreach(_ ! ClickhouseBulkActor.Completed)
    }
  }

  override def postStop(): Unit = {
    config.completionFn()
  }

}


object ClickhouseBulkActor {

  def props(targetTable: String,
               client: ClickhouseClient,
               config: SubscriberConfig,
            moreRequester: Option[ActorRef] = None): Props = {
    Props(new ClickhouseBulkActor(targetTable, client, config, moreRequester))
  }

  // signifies that the upstream publisher has completed (NOT that a bulk request has succeeded)
  case object Completed

  case object ForceIndexing

  case class Request(size: Int)

  case class Insert(table: String, data: Map[String, Any])

  case class Send(req: String, attempts: Int)

  case class FlushSuccess(result: String, count: Int)

  case class FlushFailure(ex: Throwable, count: Int)

}

class ClickhouseBulkActor(targetTable: String,
                          client: ClickhouseClient,
                          config: SubscriberConfig,
                          moreRequester: Option[ActorRef] = None) extends Actor with LazyLogging {


  logger.info(s"Starting ClickhouseBulkActor for $targetTable")

  import context.{dispatcher, system}

  private val buffer = new ArrayBuffer[ClickhouseBulkActor.Insert]()
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

  def receive: Receive = {
    case t: Throwable => handleError(t)
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
      buffer.append(m)
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

    case ClickhouseBulkActor.FlushFailure(ex, count) =>
      logger.error(s"Exception while indexing $count items", ex)
      failed = failed + count
      logger.debug(s"Clickhouse $targetTable failed: $failed, (confirmed: $confirmed)")
      config.failureCallback(targetTable, count)
      checkCompleteOrRequestNext(count)
  }

  // need to check if we're completed, because if we are then this might be the last pending conf
  // and if it is, we can shutdown.
  private def checkCompleteOrRequestNext(n: Int): Unit = {
    if (completed) shutdownIfAllConfirmed()
    else moreRequester.getOrElse(self) ! ClickhouseBulkActor.Request(n)
  }

  // Stops the schedulers if they exist
  override def postStop(): Unit = {
    flushIntervalScheduler.map(_.cancel)
    flushAfterScheduler.map(_.cancel)
  }

  private def shutdownIfAllConfirmed(): Unit = {
    if (confirmed + failed == sent) {
      context.stop(self)
    }
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

  val insertQuery = s"INSERT INTO $targetTable FORMAT JSONEachRow"

  private val objectMapper = new ObjectMapper()
      .registerModule(new JodaModule)
      .registerModule(new DefaultScalaModule)
      .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT)

  private def index(): Unit = {
    val payload = buffer.map(i => objectMapper.writeValueAsString(i.data)).mkString("\n") + "\n"
    val count = buffer.size
    logger.debug(s"Inserting $count")
    sent = sent + count
    logger.debug(s"Clickhouse $targetTable sent: $sent (confirmed: $confirmed, failed: $failed)")
    send(payload, count)
    buffer.clear()
    // buffer is now empty so no point keeping a scheduled flush after operation
    flushAfterScheduler.foreach(_.cancel)
    flushAfterScheduler = None
  }

  private def send(payload: String, count: Int, retries: Int = 3): Unit = {
    client.execute(insertQuery, payload) onComplete {
      case Failure(_) if retries > 0 => send(payload, count, retries - 1)
      case Failure(e) => self ! ClickhouseBulkActor.FlushFailure(e, count)
      case Success(resp: String) => self ! ClickhouseBulkActor.FlushSuccess(resp, count)
    }

  }

}

case class SubscriberConfig(batchSize: Int = 10000,
                            concurrentRequests: Int = 1,
                            refreshAfterOp: Boolean = false,
                            //                            listener: ResponseListener = ResponseListener.noop,
                            completionFn: () => Unit = () => (),
                            errorFn: Throwable => Unit = e => (),
                            //                            failureWait: FiniteDuration = 2.seconds,
                            //                            maxAttempts: Int = 5,
                            failureCallback: (String, Long) => Unit = (table, count) => (),
                            successCallback: (String, Long) => Unit = (table, count) => (),
                            flushInterval: Option[FiniteDuration] = None,
                            flushAfter: Option[FiniteDuration] = None)
