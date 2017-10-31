package com.crobox.clickhouse.balancing.discovery.cluster

import akka.testkit.TestProbe
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.Connections
import com.crobox.clickhouse.balancing.discovery.cluster.ClusterConnectionProviderActor.ScanHosts
import com.crobox.clickhouse.discovery.ConnectionConfig
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.crobox.clickhouse.internal.InternalExecutorActor.Execute
import com.crobox.clickhouse.test.ClickhouseClientSpec

class ClusterConnectionProviderActorTest extends ClickhouseClientSpec {

  val internalExecutor = TestProbe()
  val provider = system.actorOf(
    ClusterConnectionProviderActor.props(testActor, internalExecutor.ref))

  it should "select only cluster specific hosts" in {
    val hosts = Seq("localhost", "anotherhost")
    val cluster = "test_cluster"
    provider ! ScanHosts(
      ConnectionConfig(ClickhouseHostBuilder.toHost("localhost"), cluster))
    internalExecutor.expectMsgPF() {
      case Execute(_, query) if query.contains(cluster) =>
        internalExecutor.reply(hosts)
    }
    expectMsg(
      Connections(
        hosts.map(ClickhouseHostBuilder.toHost(_))
      ))
  }

}
