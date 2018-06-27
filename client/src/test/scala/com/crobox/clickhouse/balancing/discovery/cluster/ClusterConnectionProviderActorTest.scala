package com.crobox.clickhouse.balancing.discovery.cluster

import akka.testkit.TestProbe
import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.balancing.discovery.ConnectionConfig
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.Connections
import com.crobox.clickhouse.balancing.discovery.cluster.ClusterConnectionProviderActor.ScanHosts
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.crobox.clickhouse.internal.InternalExecutorActor.Execute

class ClusterConnectionProviderActorTest extends ClickhouseClientSpec {
  val internalExecutor = TestProbe()
  val provider         = system.actorOf(ClusterConnectionProviderActor.props(testActor, internalExecutor.ref))

  it should "select only cluster specific hosts" in {
    val hosts   = Set("localhost", "anotherhost")
    val cluster = "test_cluster"
    provider ! ScanHosts(ConnectionConfig(ClickhouseHostBuilder.toHost("localhost", Some(8123)), cluster))
    internalExecutor.expectMsgPF() {
      case Execute(_, Some(query)) if query.contains(cluster) =>
        internalExecutor.reply(hosts.toSeq)
    }
    expectMsg(
      Connections(
        hosts.map(ClickhouseHostBuilder.toHost(_, Some(8123)))
      )
    )
  }

}
