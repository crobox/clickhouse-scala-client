package com.crobox.clickhouse.balancing

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model._
import com.crobox.clickhouse.balancing.Connection.{BalancingHosts, ClusterAware, ConnectionType, SingleHost}
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.balancing.discovery.health.ClickhouseHostHealth
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
//import scala.jdk.CollectionConverters._

trait HostBalancer extends LazyLogging {
  def nextHost: Future[Uri]
}

object HostBalancer extends ClickhouseHostBuilder {

  def apply(
      optionalConfig: Option[Config] = None
  )(implicit system: ActorSystem, ec: ExecutionContext): HostBalancer = {
    val config                   = optionalConfig.getOrElse(system.settings.config)
    val connectionConfig         = config.getConfig("connection")
    val connectionType           = ConnectionType(connectionConfig.getString("type"))
    val connectionHostFromConfig = extractHost(connectionConfig)
    connectionType match {
      case SingleHost => SingleHostBalancer(connectionHostFromConfig)
      case BalancingHosts =>
        val manager = system.actorOf(ConnectionManagerActor.props(ClickhouseHostHealth.healthFlow(_)))
        MultiHostBalancer(connectionConfig
                            .getConfigList("hosts")
                            .asScala
                            .toSet
                            .map((config: Config) => extractHost(config)),
                          manager)
      case ClusterAware =>
        val manager = system.actorOf(ConnectionManagerActor.props(ClickhouseHostHealth.healthFlow(_)))
        ClusterAwareHostBalancer(
          connectionHostFromConfig,
          connectionConfig.getString("cluster"),
          manager,
          connectionConfig.getDuration("scanning-interval").getSeconds.seconds
        )(system, config.getDuration("host-retrieval-timeout").getSeconds.seconds, ec)
    }
  }

  def extractHost(connectionConfig: Config): Uri =
    toHost(connectionConfig.getString("host"),
           if (connectionConfig.hasPath("port")) Option(connectionConfig.getInt("port")) else None)
}
