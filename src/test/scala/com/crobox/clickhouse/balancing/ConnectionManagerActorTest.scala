package com.crobox.clickhouse.balancing

import java.util.UUID

import akka.actor.{ActorNotFound, ActorRef}
import akka.http.scaladsl.model.Uri
import akka.pattern.ask
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.Connections
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.{
  Alive,
  Dead
}
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.crobox.clickhouse.test.ClickhouseClientAsyncSpec
import org.scalatest.Assertion

import scala.concurrent.duration._

class ConnectionManagerActorTest extends ClickhouseClientAsyncSpec {

  private val hostUris: Seq[Uri] = uris.keySet.toSeq
  it should "remove dead connection" in {
    val urisWithDead = uris.+(
      (ClickhouseHostBuilder
         .toHost("deadConnection"),
       HostAliveMock.props(Seq(Dead)) _))
    val manager =
      system.actorOf(
        ConnectionManagerActor.props(uri => urisWithDead(uri)(uri), config))
    (manager ? Connections(urisWithDead.keySet.toSeq)).flatMap(_ => {
      returnsConnectionsInRoundRobinFashion(manager, uris.keySet)
    })
  }

  it should "add back connection when it comes to life" in {
    val urisWithDead = uris.+(
      (ClickhouseHostBuilder
         .toHost("deadConnection"),
       HostAliveMock.props(Seq(Dead, Alive, Alive)) _))
    val manager =
      system.actorOf(
        ConnectionManagerActor.props(uri => urisWithDead(uri)(uri), config))

    (manager ? Connections(urisWithDead.keySet.toSeq))
      .flatMap(_ => {
        returnsConnectionsInRoundRobinFashion(manager, uris.keySet)
      })
      .flatMap(_ => {
        //        TODO remove the sleeps by injecting test probes as health actors
        Thread.sleep(1100)
        returnsConnectionsInRoundRobinFashion(manager, urisWithDead.keySet)
      })
  }

  it should "kill health actor when connection is removed from configuration" in {
    val managerName = UUID.randomUUID().toString
    val manager =
      system.actorOf(
        ConnectionManagerActor.props(uri => uris(uri)(uri), config),
        managerName)

    import system.dispatcher
    (manager ? Connections(hostUris)).flatMap(_ => {
      val droppedHost = hostUris.head
      (manager ? Connections(hostUris.drop(1))).flatMap(_ => {
        Thread.sleep(2000)
        system
          .actorSelection(
            s"/user/$managerName/${ConnectionManagerActor.healthCheckActorName(droppedHost)}")
          .resolveOne(1 second)
          .recoverWith {
            case _: ActorNotFound => succeed
          }
          .map {
            case _: ActorRef          => fail("Actor for health check exists")
            case assertion: Assertion => assertion
          }
      })
    })

  }

  private def returnsConnectionsInRoundRobinFashion(
      manager: ActorRef,
      expectedConnections: Set[Uri]) = {
    import system.dispatcher
    val RequestConnectionsPerHost = 1000
    getConnections(manager, RequestConnectionsPerHost * expectedConnections.size)
      .map(connections => {
        val connectionsPerHost = expectedConnections.toSeq
          .map(uri => connections.count(_ == uri))
        val expectedConnectionsPerHost = expectedConnections.toSeq.map(_ => RequestConnectionsPerHost)
        connectionsPerHost should contain theSameElementsAs expectedConnectionsPerHost
      })
  }

}
