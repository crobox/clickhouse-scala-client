package com.crobox.clickhouse.balancing.discovery

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.http.scaladsl.model.Uri
import akka.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.{
  Connections,
  GetConnection
}
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.{
  Alive,
  Dead
}
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{
  HostStatus,
  IsAlive
}
import com.crobox.clickhouse.balancing.iterator.CircularIterator
import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.duration._

class ConnectionManagerActor(healthProvider: (Uri) => Props, config: Config)
    extends Actor
    with ActorLogging {
  private implicit val timeout = durationToTimeout(5 second)

  var connectionIterator: CircularIterator[Uri] = new CircularIterator[Uri]()
  val hostsStatus = mutable.Map.empty[Uri, HostStatus]
  val healthCheckInterval = config
    .getLong("com.crobox.clickhouse.client.connection.health-check-interval") seconds
  override def receive = {
    case Connections(hosts) =>
      hosts.toParArray
        .foreach(host => {
          hostsStatus.getOrElse(
            host, {
//              TODO kill health actors for removed hosts
              val hostHealthChecker: ActorRef = context.actorOf(
                healthProvider(host),
                s"${host.authority.host.address()}:${host.authority.port}")
              log.info(s"Setting up host health checks for host $host")

              context.system.scheduler
                .schedule(Duration.Zero,
                          healthCheckInterval,
                          hostHealthChecker,
                          IsAlive())(context.dispatcher, context.self)
            }
          )
        })
      sender ! Unit

    case GetConnection() =>
      val uri = connectionIterator.next()
      sender ! uri

    case status @ HostStatus(host, Alive) =>
      logHostStatus(status, host)
      connectionIterator.add(host)
      hostsStatus.put(host, status)

    case status @ HostStatus(host, Dead) =>
      logHostStatus(status, host)
      connectionIterator.remove(host)
      hostsStatus.put(host, status)
  }

  private def logHostStatus(status: HostStatus, host: Uri) = {
    if (!hostsStatus.contains(host)) {
      log.info(s"Adding host status $status")
    } else {
      if (hostsStatus(host) != status) {
        log.info(s"Updating host status from ${hostsStatus(host)} to $status")
      }
    }
  }
}

object ConnectionManagerActor {

  def props(healthProvider: (Uri) => Props, config: Config): Props =
    Props(new ConnectionManagerActor(healthProvider, config))

  case class GetConnection()
  case class Connections(hosts: Seq[Uri])
}
