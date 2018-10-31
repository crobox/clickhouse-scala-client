package com.crobox.clickhouse.balancing

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.Materializer
import com.crobox.clickhouse.balancing.Connection.{BalancingHosts, ClusterAware, ConnectionType, SingleHost}
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.balancing.discovery.health.ClickhouseHostHealth
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait HostBalancer extends LazyLogging {

  def nextHost: Future[Uri]

}

object HostBalancer extends ClickhouseHostBuilder {
  val ConnectionConfigPrefix = "crobox.clickhouse.client.connection"

  def apply(
      config: Config
  )(implicit system: ActorSystem, materializer: Materializer, ec: ExecutionContext): HostBalancer = {
    val connectionConfig =
      config.getConfig(ConnectionConfigPrefix)
    val connectionType           = ConnectionType(connectionConfig.getString("type"))
    val connectionHostFromConfig = extractHost(connectionConfig)
    connectionType match {
      case SingleHost => SingleHostBalancer(connectionHostFromConfig)
      case BalancingHosts =>
        val manager = system.actorOf(
          ConnectionManagerActor
            .props(ClickhouseHostHealth.healthFlow(_), config)
        )
        MultiHostBalancer(connectionConfig
                            .getConfigList("hosts")
                            .asScala
                            .toSet
                            .map((config: Config) => extractHost(config)),
                          manager)
      case ClusterAware =>
        val manager = system.actorOf(
          ConnectionManagerActor
            .props(ClickhouseHostHealth.healthFlow(_), config)
        )
        ClusterAwareHostBalancer(
          connectionHostFromConfig,
          connectionConfig.getString("cluster"),
          manager,
          connectionConfig.getDuration("scanning-interval").getSeconds seconds
        )(system,
          config.getDuration("crobox.clickhouse.client.host-retrieval-timeout").getSeconds seconds,
          ec,
          materializer)
    }
  }

  def extractHost(connectionConfig: Config): Uri =
    toHost(connectionConfig.getString("host"),
           if (connectionConfig.hasPath("port")) Option(connectionConfig.getInt("port")) else None)
}
