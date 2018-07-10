package com.crobox.clickhouse.stream

import akka.Done
import akka.actor.{Actor, ActorRef, ActorRefFactory, PoisonPill, Props, Terminated}
import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.stream.ClickhouseBulkActor.ClickhouseIndexingException
import com.typesafe.scalalogging.LazyLogging
import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}


object ClickhouseIndexingSubscriber extends LazyLogging {

  /**
   * @param client - clickhouse client
   * @param flushInterval - how often to flush(recommended no more than once per second)
   * @param failureCallback - called on every INSERT batch failure with info about (table, batchSize)
   * @param successCallback - called on every INSERT batch success with info about (table, batchSize)
   */
  def default(
      client: ClickhouseClient,
      flushInterval: Option[FiniteDuration],
      failureCallback: (ClickhouseIndexingException) => Unit = (ex) => (),
      successCallback: (String, Long) => Unit = (table, count) => ()
  )(implicit actorRefFactory: ActorRefFactory): (Subscriber[ClickhouseBulkActor.Insert], Future[Done]) = {
    val completed = Promise[Done]
    val clickSubscriber = new ClickhouseIndexingSubscriber(
      client,
      SubscriberConfig(
        batchSize = 64 * 1024,
        errorFn = e => {
          logger.error(e.getMessage, e)
        },
        completionFn = () => {
          logger.debug(s"Completed!")
          completed.success(Done)
        },
        failureCallback = (ex) => {
          failureCallback(ex)
        },
        successCallback = (table, count) => {
          successCallback(table, count)
        },
        flushInterval = flushInterval
      )
    )
    (clickSubscriber, completed.future)
  }
}

/**
 * @author Sjoerd Mulder
 * @since 16-9-16
  * @deprecated 18-07-09 - Use `ClickhouseSink.sink` for the akka streams implementation
  *
 */
class ClickhouseIndexingSubscriber(client: ClickhouseClient, config: SubscriberConfig)(
    implicit actorRefFactory: ActorRefFactory
) extends Subscriber[ClickhouseBulkActor.Insert] {

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

  override def onComplete(): Unit =
    actor ! ClickhouseBulkActor.Completed

  def close(): Unit =
    actor ! PoisonPill
}

class ClickhouseBulkActorManager(client: ClickhouseClient, subscription: Subscription, config: SubscriberConfig)
    extends Actor
    with LazyLogging {

  private def getActor(table: String): ActorRef =
    context
      .child(table)
      .getOrElse({
        val child = context.actorOf(ClickhouseBulkActor.props(table, client, config, Some(self)), table)
        context.watch(child)
        child
      })

  // total number of documents requested from our publisher
  private var requested: Long = 0L

  // requests our initial starting batches, we can request them all at once, and then just request a new batch
  // each time we complete a batch
  override def preStart(): Unit =
    self ! ClickhouseBulkActor.Request(config.batchSize * config.concurrentRequests)

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

  private def shutdown(downChildren: Boolean = false) =
    if (context.children.isEmpty) {
      context.stop(self)
    } else if (downChildren) {
      context.children.foreach(_ ! ClickhouseBulkActor.Completed)
    }

  override def postStop(): Unit =
    config.completionFn()

}

case class SubscriberConfig(batchSize: Int = 10000,
                            concurrentRequests: Int = 1,
                            refreshAfterOp: Boolean = false,
                            //                            listener: ResponseListener = ResponseListener.noop,
                            completionFn: () => Unit = () => (),
                            errorFn: Throwable => Unit = e => (),
                            //                            failureWait: FiniteDuration = 2.seconds,
                            //                            maxAttempts: Int = 5,
                            failureCallback: (ClickhouseIndexingException) => Unit = (ex) => {},
                            successCallback: (String, Long) => Unit = (table, count) => (),
                            flushInterval: Option[FiniteDuration] = None,
                            flushAfter: Option[FiniteDuration] = None)
