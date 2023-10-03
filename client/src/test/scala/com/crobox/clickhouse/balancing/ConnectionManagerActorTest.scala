package com.crobox.clickhouse.balancing

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorRef, Cancellable, PoisonPill}
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.{Source, SourceQueueWithComplete}
import org.apache.pekko.testkit.TestProbe
import com.crobox.clickhouse.ClickhouseClientAsyncSpec
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.{Connections, GetConnection}
import com.crobox.clickhouse.balancing.discovery.health.ClickhouseHostHealth
import com.crobox.clickhouse.balancing.discovery.health.ClickhouseHostHealth.{Alive, ClickhouseHostStatus}
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.typesafe.config.ConfigValueFactory
import org.scalatest.concurrent.Eventually

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class ConnectionManagerActorTest extends ClickhouseClientAsyncSpec with Eventually {
  private val host1 = ClickhouseHostBuilder
    .toHost("localhost", Some(8245))
  private val host2 = ClickhouseHostBuilder
    .toHost("127.0.0.1", None)
  private val host3 = ClickhouseHostBuilder
    .toHost("thirdsHost", None)

  var uris: Map[Uri, (SourceQueueWithComplete[ClickhouseHostStatus], Source[ClickhouseHostStatus, Cancellable])] = _
  var manager: ActorRef                                                                                          = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    uris = hosts()
    manager = system.actorOf(ConnectionManagerActor.props(uri => uris(uri)._2))
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    uris.values.foreach(_._1.complete())
    manager ! PoisonPill
  }

  "Connection manager" should "remove connection with failed health check" in {
    Future
      .sequence(
        Seq(
          uris(host1)._1.offer(Alive(host1)),
          uris(host2)._1.offer(Alive(host2)),
          uris(host3)._1.offer(Alive(host3)),
          uris(host3)._1.offer(ClickhouseHostHealth.Dead(host3, new IllegalArgumentException("Got it wrong")))
        )
      )
      .flatMap(_ => {
        manager ! Connections(uris.keySet)
        ensureCompleted(uris).flatMap(_ => {
          returnsConnectionsInRoundRobinFashion(manager, uris.keySet.-(host3))
        })
      })
  }

  it should "add back connection when health check passes" in {
    uris(host1)._1.offer(Alive(host1))
    uris(host2)._1.offer(Alive(host2))
    uris(host3)._1.offer(ClickhouseHostHealth.Dead(host3, new IllegalArgumentException))
    manager ! Connections(uris.keySet)
    ensureCompleted(uris - host3)
      .flatMap(_ => returnsConnectionsInRoundRobinFashion(manager, uris.keySet.-(host3)))
      .flatMap(_ => {
        uris(host3)._1.offer(ClickhouseHostHealth.Alive(host3))
        ensureCompleted(uris.filter(_._1 == host3))
          .flatMap(_ => returnsConnectionsInRoundRobinFashion(manager, uris.keySet))
      })
  }

  it should "cancel health source when connection is removed from configuration" in {
    manager ! Connections(uris.keySet)
    val droppedHost         = uris.head
    val hostUrisWithDropped = uris.drop(1)
    manager ! Connections(hostUrisWithDropped.keySet)
    droppedHost._2._1.offer(Alive(droppedHost._1))
    droppedHost._2._1.watchCompletion().map(_ => succeed)
  }

  it should "stash messages when no connections were received yet" in {
    val client = TestProbe()
    manager.tell(GetConnection(), client.ref)
    val uri = uris.keySet.head
    uris(uri)._1.offer(Alive(uri))
    manager ! Connections(Set(uri))
    Future {
      client.expectMsg(1 second, uri)
      succeed
    }
  }
  it should "return config connection when no connections were received yet" in {
    val client = TestProbe()
    val host   = "default-mega-host"
    val manager =
      system.actorOf(
        ConnectionManagerActor.props(
          uri => uris(uri)._2,
          Some(config
            .getConfig("crobox.clickhouse.client")
            .withValue("connection.fallback-to-config-host-during-initialization", ConfigValueFactory.fromAnyRef(true))
            .withValue("connection.host", ConfigValueFactory.fromAnyRef(host))
          )
        )
      )
    manager.tell(GetConnection(), client.ref)
    Future {
      client.expectMsg(1 second, ClickhouseHostBuilder.toHost(host, Some(8123)))
      succeed
    }
  }

  private def hosts() =
    Map(
      host1 -> statusesAsSource(),
      host2 -> statusesAsSource(),
      host3 -> statusesAsSource()
    )

  private def statusesAsSource()
    : (SourceQueueWithComplete[ClickhouseHostStatus], Source[ClickhouseHostStatus, Cancellable]) = {
    val (queue, source) = Source
      .queue[ClickhouseHostStatus](10, OverflowStrategy.fail)
      .preMaterialize()
    (queue, source.mapMaterializedValue(_ => {
      new Cancellable {
        override def cancel(): Boolean = {
          queue.complete()
          true
        }
        override def isCancelled: Boolean = queue.watchCompletion().isCompleted
      }
    }))
  }

  private def ensureCompleted(
      uris: Map[Uri, (SourceQueueWithComplete[ClickhouseHostStatus], Source[ClickhouseHostStatus, Cancellable])]
  ): Future[Iterable[Done]] =
    Future.sequence(uris.values.map(queue => {
      queue._1.complete()
      queue._1.watchCompletion()
    }) ++ Seq(Future {
      Thread.sleep(1000)//FIXME find a cleaner way to ensure the manager processes all the elements from the stream
      Done
    }))

}
