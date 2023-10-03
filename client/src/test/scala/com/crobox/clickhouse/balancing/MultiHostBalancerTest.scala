package com.crobox.clickhouse.balancing

import org.apache.pekko.testkit.TestProbe
import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.Connections
import com.crobox.clickhouse.internal.ClickhouseHostBuilder

class MultiHostBalancerTest extends ClickhouseClientSpec {

  val uris = Set(ClickhouseHostBuilder.toHost("element", None))
  it should "pass the hosts to the manager" in {
    val manager = TestProbe()
    MultiHostBalancer(uris, manager.ref)
    manager.expectMsg(Connections(uris))
  }

}
