package com.crobox.clickhouse.balancing

import com.crobox.clickhouse.ClickhouseClientAsyncSpec
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.IsAlive

class MultiHostBalancerTest extends ClickhouseClientAsyncSpec {

  it should "round robin the configured hosts" in {
    val manager =
      system.actorOf(ConnectionManagerActor.props(uri => uris(uri)(uri), config))
    val balancer             = MultiHostBalancer(uris.keySet, manager)
    val RequestedConnections = 100
    probe.expectMsgAllOf(IsAlive(), IsAlive())
    requestParallelHosts(balancer, RequestedConnections).map(results => {
      eachHostReceivedExpectedConnections(results, uris.keySet, RequestedConnections / uris.keySet.size)
    })
  }

}
