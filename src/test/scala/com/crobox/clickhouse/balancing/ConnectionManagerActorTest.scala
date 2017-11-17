package com.crobox.clickhouse.balancing

import java.util.UUID

import akka.actor.{ActorNotFound, ActorRef}
import akka.pattern.ask
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.Connections
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.{Alive, Dead}
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.crobox.clickhouse.test.ClickhouseClientAsyncSpec
import org.scalatest.Assertion

import scala.concurrent.duration._

class ConnectionManagerActorTest extends ClickhouseClientAsyncSpec {

  it should "remove dead connection" in {
    val urisWithDead = uris.+(
      (ClickhouseHostBuilder
         .toHost("deadConnection", None),
       HostAliveMock.props(Seq(Dead)) _)
    )
    val manager =
      system.actorOf(ConnectionManagerActor.props(uri => urisWithDead(uri)(uri), config))
    (manager ? Connections(urisWithDead.keySet)).flatMap(_ => {
      returnsConnectionsInRoundRobinFashion(manager, uris.keySet)
    })
  }

  it should "add back connection when it comes to life" in {
    val urisWithDead = uris.+(
      (ClickhouseHostBuilder
         .toHost("deadConnection", None),
       HostAliveMock.props(Seq(Dead, Alive, Alive, Alive, Alive)) _)
    )
    val manager =
      system.actorOf(ConnectionManagerActor.props(uri => urisWithDead(uri)(uri), config))
    (manager ? Connections(urisWithDead.keySet))
      .flatMap(_ => {
        probe.receiveN(3, 2 seconds)
        returnsConnectionsInRoundRobinFashion(manager, uris.keySet)
      })
      .flatMap(_ => {
        probe.receiveN(3, 2 seconds)
        returnsConnectionsInRoundRobinFashion(manager, urisWithDead.keySet)
      })
  }

  it should "kill health actor when connection is removed from configuration" in {
    val managerName = UUID.randomUUID().toString
    val manager =
      system.actorOf(ConnectionManagerActor.props(uri => uris(uri)(uri), config), managerName)
    val hostUris = uris.keySet
    import system.dispatcher
    (manager ? Connections(hostUris)).flatMap(_ => {
      probe.receiveN(hostUris.size, 2 seconds)
      val droppedHost         = hostUris.head
      val hostUrisWithDropped = hostUris.drop(1)
      (manager ? Connections(hostUrisWithDropped)).flatMap(_ => {
        probe.receiveN(2, 2 seconds)
        system
          .actorSelection(s"/user/$managerName/${ConnectionManagerActor.healthCheckActorName(droppedHost)}")
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

}
