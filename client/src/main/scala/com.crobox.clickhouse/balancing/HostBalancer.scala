package com.crobox.clickhouse.balancing

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import com.crobox.clickhouse.balancing.Connection.{BalancingHosts, ClusterAware, ConnectionType, SingleHost}
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.balancing.discovery.cluster.ClusterConnectionProviderActor
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker
import com.crobox.clickhouse.internal.{ClickhouseHostBuilder, InternalExecutorActor}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._

trait HostBalancer extends LazyLogging {

  def nextHost: Future[Uri]

}

object HostBalancer extends ClickhouseHostBuilder {
  val ConnectionConfigPrefix = "crobox.clickhouse.client.connection"

  def apply(config: Config)(implicit system: ActorSystem): HostBalancer = {
    val connectionConfig =
      config.getConfig(ConnectionConfigPrefix)
    val internalExecutor         = system.actorOf(InternalExecutorActor.props(config))
    val connectionType           = ConnectionType(connectionConfig.getString("type"))
    val connectionHostFromConfig = extractHost(connectionConfig)
    connectionType match {
      case SingleHost => SingleHostBalancer(connectionHostFromConfig)
      case BalancingHosts =>
        val manager = system.actorOf(
          ConnectionManagerActor
            .props(HostHealthChecker.props(
                     _,
                     internalExecutor,
                     connectionConfig.getDuration("health-check.timeout").getSeconds seconds
                   ),
                   config)
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
            .props(HostHealthChecker.props(
                     _,
                     internalExecutor,
                     connectionConfig.getDuration("health-check.timeout").getSeconds seconds
                   ),
                   config)
        )
        val provider = system.actorOf(ClusterConnectionProviderActor.props(manager, internalExecutor))
        ClusterAwareHostBalancer(
          connectionHostFromConfig,
          connectionConfig.getString("cluster"),
          manager,
          provider,
          connectionConfig.getDuration("scanning-interval").getSeconds seconds
        )(system, config.getDuration("crobox.clickhouse.client.host-retrieval-timeout").getSeconds seconds)
    }
  }

  def extractHost(connectionConfig: Config): Uri =
    toHost(connectionConfig.getString("host"),
           if (connectionConfig.hasPath("port")) Option(connectionConfig.getInt("port")) else None)
}
