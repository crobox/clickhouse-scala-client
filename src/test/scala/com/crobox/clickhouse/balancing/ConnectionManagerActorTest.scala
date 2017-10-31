package com.crobox.clickhouse.balancing

import java.util.UUID

import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.{
  Alive,
  Dead
}
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.crobox.clickhouse.test.ClickhouseClientAsyncSpec
import akka.pattern.ask
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.Connections

class ConnectionManagerActorTest extends ClickhouseClientAsyncSpec {

  it should "remove dead connection" in {
    val urisWithDead = uris.+(
      (ClickhouseHostBuilder
         .toHost("deadConnection"),
       HostAliveMock.props(Seq(Dead)) _))
    val manager =
      system.actorOf(
        ConnectionManagerActor.props(uri => urisWithDead(uri)(uri), config),
        s"manager-${UUID.randomUUID()}")
    import system.dispatcher
    (manager ? Connections(urisWithDead.keySet.toSeq)).flatMap(_ => {
      getConnections(manager, 100).map(connections => {
        connections should contain theSameElementsAs elementsDuplicated(
          uris.keySet.toSeq,
          100)
      })
    })
  }

  it should "add back connection when it comes to life" in {
    val urisWithDead = uris.+(
      (ClickhouseHostBuilder
         .toHost("deadConnection"),
       HostAliveMock.props(Seq(Dead, Alive, Alive)) _))
    val manager =
      system.actorOf(
        ConnectionManagerActor.props(uri => urisWithDead(uri)(uri), config),
        s"manager-${UUID.randomUUID()}")

    (manager ? Connections(urisWithDead.keySet.toSeq))
      .flatMap(_ => {
        getConnections(manager, 100)
          .flatMap(
            _ should contain theSameElementsAs elementsDuplicated(
              uris.keySet.toSeq,
              100))
      })
      .flatMap(_ => {
        Thread.sleep(1100)
        getConnections(manager, 120).map(
          _ should contain allElementsOf elementsDuplicated(
            urisWithDead.keySet.toSeq,
            110))
      })
  }
}
