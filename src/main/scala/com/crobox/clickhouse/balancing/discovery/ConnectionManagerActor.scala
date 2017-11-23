package com.crobox.clickhouse.balancing.discovery

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, PoisonPill, Props, Status}
import akka.http.scaladsl.model.Uri
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.{Alive, Dead}
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{HostStatus, IsAlive}
import com.crobox.clickhouse.balancing.iterator.CircularIteratorSet
import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.duration._

class ConnectionManagerActor(healthProvider: (Uri) => Props, config: Config) extends Actor with ActorLogging {

  import ConnectionManagerActor._

  private val healthCheckInterval: FiniteDuration =
    config
      .getDuration("com.crobox.clickhouse.client.connection.health-check.interval")
      .getSeconds seconds
  private val hostRetrievalTimeout =
    config
      .getDuration("com.crobox.clickhouse.client.host-retrieval-timeout")
      .getSeconds seconds

  //  state
  val connectionIterator: CircularIteratorSet[Uri] =
    new CircularIteratorSet[Uri]()
  val hostsStatus                      = mutable.Map.empty[Uri, HostStatus]
  val hostHealthScheduler              = mutable.Map.empty[Uri, Cancellable]
  var currentConfiguredHosts: Set[Uri] = Set.empty

  override def receive = {
    case Connections(hosts) =>
      hosts
        .foreach(host => {
          if (!currentConfiguredHosts.contains(host)) {
            val hostHealthChecker: ActorRef =
              context.actorOf(healthProvider(host), healthCheckActorName(host))
            log.info(s"Setting up host health checks for host $host")
            val scheduler = context.system.scheduler
              .schedule(Duration.Zero, healthCheckInterval, hostHealthChecker, IsAlive())(context.dispatcher,
                                                                                          context.self)
            hostHealthScheduler.put(host, scheduler)
          }
        })
      currentConfiguredHosts = hosts
      sender ! Unit

    case GetConnection() =>
      if (connectionIterator.hasNext) {
        val uri = connectionIterator.next()
        sender ! uri
      } else {
        sender ! Status.Failure(
          NoHostAvailableException(s"No connection is available. Current connections statuses $hostsStatus")
        )
      }

    case status @ HostStatus(host, _) =>
      if (currentConfiguredHosts.contains(host)) {
        logHostStatus(status)
        hostsStatus.put(host, status)
        status.status match {
          case Alive => connectionIterator.add(host)
          case Dead  => connectionIterator.remove(host)
        }
      } else {
        log.info(
          s"Received host status $status for host which is no longer enabled for this connection. Killing health check actor for it."
        )
        sender ! PoisonPill
        cleanUpHost(host)
      }
  }

  private def cleanUpHost(host: Uri) = {
    hostsStatus.remove(host)
    connectionIterator.remove(host)
    hostHealthScheduler.get(host).foreach(_.cancel())
    hostHealthScheduler.remove(host)
  }

  private def logHostStatus(status: HostStatus) {
    val host = status.host
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

  def healthCheckActorName(host: Uri) =
    s"${host.authority.host.address()}:${host.authority.port}"

  case class GetConnection()

  case class Connections(hosts: Set[Uri])

  case class NoHostAvailableException(msg: String) extends IllegalStateException(msg)
}
