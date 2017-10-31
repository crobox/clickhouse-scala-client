package com.crobox.clickhouse.balancing

import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.crobox.clickhouse.test.ClickhouseClientSpec
import com.typesafe.config.ConfigFactory

class HostBalancerTest extends ClickhouseClientSpec {

  it should "resolve to single host balancer" in {
    HostBalancer(config) match {
      case SingleHostBalancer(host) =>
        host shouldEqual ClickhouseHostBuilder.toHost("localhost")
    }
  }

  it should "resolve to multi host balancer" in {
    HostBalancer(ConfigFactory.parseString("""com.crobox.clickhouse.client {
        |    connection: {
        |        type: "balancing-hosts"
        |        hosts: [
        |          {
        |            host: "localhost",
        |            port: 8123
        |          }
        |        ]
        |        health-check-interval = 1
        |    }
        |}
      """.stripMargin)) match {
      case MultiHostBalancer(hosts, _) =>
        hosts should contain theSameElementsInOrderAs Seq(
          ClickhouseHostBuilder.toHost("localhost"))
    }
  }

  it should "resolve to cluster aware host balancer" in {
    HostBalancer(ConfigFactory.parseString("""com.crobox.clickhouse.client {
        |    connection: {
        |        type: "cluster-aware"
        |        host: "localhost"
        |        port: 8123
        |        cluster: "cluster"
        |        health-check-interval = 1
        |    }
        |}
      """.stripMargin)) match {
      case ClusterAwareHostBalancer(host, cluster, _, _) =>
        host shouldEqual ClickhouseHostBuilder.toHost("localhost")
        cluster shouldBe "cluster"
    }
  }

}
