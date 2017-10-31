package com.crobox.clickhouse.balancing

import java.util.UUID

import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.test.ClickhouseClientAsyncSpec

class MultiHostBalancerTest extends ClickhouseClientAsyncSpec {

  it should "round robin the configured hosts" in {
    val manager =
      system.actorOf(
        ConnectionManagerActor.props(uri => uris(uri)(uri), config),
        s"manager-${UUID.randomUUID()}")

    val balancer = MultiHostBalancer(uris.keys.toSeq, manager)
    val RequestedConnections = 100
    requestParallelHosts(balancer, RequestedConnections).map(results => {
      eachHostReceivedExpectedConnections(
        results,
        uris.keySet,
        RequestedConnections / uris.keySet.size)
    })
  }

}
