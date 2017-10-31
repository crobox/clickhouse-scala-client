package com.crobox.clickhouse.balancing

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import com.crobox.clickhouse.balancing.Connection.{
  BalancingHosts,
  ClusterAware,
  ConnectionType,
  SingleHost
}
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.balancing.discovery.cluster.ClusterConnectionProviderActor
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker
import com.crobox.clickhouse.internal.{
  ClickhouseHostBuilder,
  InternalExecutorActor
}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions._
import scala.concurrent.Future

trait HostBalancer extends LazyLogging {

  def nextHost: Future[Uri]

}
object HostBalancer extends ClickhouseHostBuilder {
  def apply(config: Config)(implicit system: ActorSystem): HostBalancer = {
    val connectionConfig =
      config.getConfig("com.crobox.clickhouse.client.connection")
    val internalExecutor = system.actorOf(InternalExecutorActor.props(config))
    val connectionType = ConnectionType(connectionConfig.getString("type"))
    connectionType match {
      case SingleHost => SingleHostBalancer(extractHost(connectionConfig))
      case BalancingHosts =>
        val manager = system.actorOf(
          ConnectionManagerActor
            .props(HostHealthChecker.props(_, internalExecutor), config))
        MultiHostBalancer(connectionConfig
                            .getConfigList("hosts")
                            .map(config => extractHost(config)),
                          manager)
      case ClusterAware =>
        val manager = system.actorOf(
          ConnectionManagerActor
            .props(HostHealthChecker.props(_, internalExecutor), config))
        val provider = system.actorOf(
          ClusterConnectionProviderActor.props(manager, internalExecutor))
        ClusterAwareHostBalancer(extractHost(connectionConfig),
                                 connectionConfig.getString("cluster"),
                                 manager,
                                 provider)
    }
  }

  private def extractHost(connectionConfig: Config): Uri = {
    toHost(connectionConfig.getString("host"), connectionConfig.getInt("port"))
  }
}
