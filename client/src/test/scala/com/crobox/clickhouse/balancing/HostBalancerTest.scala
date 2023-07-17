package com.crobox.clickhouse.balancing

import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class HostBalancerTest extends ClickhouseClientSpec {

  it should "resolve to single host balancer" in {
    HostBalancer() match {
      case SingleHostBalancer(host) =>
        host shouldEqual ClickhouseHostBuilder.toHost("localhost", Some(8123))
    }
  }

  it should "resolve to multi host balancer" in {
    HostBalancer(Some(ConfigFactory.parseString("""
        |    connection: {
        |        type: "balancing-hosts"
        |        hosts: [
        |          {
        |            host: "localhost",
        |            port: 8123
        |          }
        |        ]
        |        health-check {
        |         timeout = 1 second
        |         interval = 1 second
        |        }
        |    }
        |
      """.stripMargin).withFallback(config.getConfig("crobox.clickhouse.client")))) match {
      case MultiHostBalancer(hosts, _) =>
        hosts.toSeq should contain theSameElementsInOrderAs Seq(ClickhouseHostBuilder.toHost("localhost", Some(8123)))
    }
  }

  it should "resolve to cluster aware host balancer" in {
    HostBalancer(Some(ConfigFactory.parseString("""
        |    connection: {
        |        type: "cluster-aware"
        |        host: "localhost"
        |        port: 8123
        |        cluster: "cluster"
        |        scanning-interval = 1 second
        |        health-check {
        |         timeout = 1 second
        |         interval = 1 second
        |        }
        |    }
        |
      """.stripMargin).withFallback(config.getConfig("crobox.clickhouse.client")))) match {
      case ClusterAwareHostBalancer(host, cluster, _, builtTimeout) =>
        host shouldEqual ClickhouseHostBuilder.toHost("localhost", Some(8123))
        cluster shouldBe "cluster"
        builtTimeout shouldBe 1.second
    }
  }

}
