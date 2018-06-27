package com.crobox.clickhouse.balancing.discovery.cluster

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import akka.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.discovery.ConnectionConfig
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.Connections
import com.crobox.clickhouse.balancing.discovery.cluster.ClusterConnectionProviderActor.ScanHosts
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.crobox.clickhouse.internal.InternalExecutorActor.Execute

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class ClusterConnectionProviderActor(manager: ActorRef, executor: ActorRef) extends Actor with ActorLogging {
  require(manager != null, "Manager has to be provided")
  require(executor != null, "Executor has to be provided")

  private implicit val timeout: Timeout                = durationToTimeout(5 second)
  private implicit val materialzier: ActorMaterializer = ActorMaterializer()

  import context.dispatcher

  override def receive = {
    case ScanHosts(config) =>
      val eventualConnections = resolveHosts(config)
      eventualConnections.onComplete {
        case Success(connection) => log.debug(s"Got successful connections $connection")
        case Failure(ex)         => log.error(ex, "Exception while getting connections")
      }
      eventualConnections pipeTo manager
  }

  def resolveHosts(config: ConnectionConfig): Future[Connections] =
    (executor ? Execute(config.host, s"SELECT host_address FROM system.clusters WHERE cluster='${config.cluster}'"))
      .mapTo[Seq[String]]
      .map(_.toSet)
      .map(result => {
        if (result.isEmpty) {
          throw new IllegalArgumentException(
            s"Could not determine clickhouse cluster hosts from $config. " +
            s"This could indicate that you are trying to use the cluster balancer to connect to a non cluster based clickhouse server. " +
            s"Please use the `SingleHostQueryBalancer` in that case."
          )
        }
        Connections(result.map(ClickhouseHostBuilder.toHost(_, Some(8123))))
      })
}

object ClusterConnectionProviderActor {

  case class ScanHosts(config: ConnectionConfig)

  def props(manager: ActorRef, executor: ActorRef) =
    Props(new ClusterConnectionProviderActor(manager, executor))
}
