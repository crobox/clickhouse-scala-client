package com.crobox.clickhouse.balancing.discovery

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, PoisonPill, Props, Stash, Status}
import akka.http.scaladsl.model.Uri
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.{Alive, Dead}
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{HostStatus, IsAlive}
import com.crobox.clickhouse.balancing.iterator.CircularIteratorSet
import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.duration._

class ConnectionManagerActor(healthProvider: (Uri) => Props, config: Config)
    extends Actor
    with ActorLogging
    with Stash {

  import ConnectionManagerActor._

  private val healthCheckInterval: FiniteDuration =
    config
      .getDuration(s"${HostBalancer.ConnectionConfigPrefix}.health-check.interval")
      .getSeconds seconds
  private val configHost: Uri = HostBalancer.extractHost(config.getConfig(HostBalancer.ConnectionConfigPrefix))
  private val fallbackToConfigurationHost =
    config.getBoolean(s"${HostBalancer.ConnectionConfigPrefix}.fallback-to-config-host-during-initialization")

  //  state
  val connectionIterator: CircularIteratorSet[Uri] =
    new CircularIteratorSet[Uri]()
  val hostsStatus                      = mutable.Map.empty[Uri, HostStatus]
  val hostHealthScheduler              = mutable.Map.empty[Uri, Cancellable]
  var currentConfiguredHosts: Set[Uri] = Set.empty
  var initialized                      = false

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

    case GetConnection() =>
      if (!initialized) {
        if (fallbackToConfigurationHost) {
          log.warning("Not yet initialized, returning the config host.")
          sender ! configHost
        } else {
          log.warning("Stashing get connection message until connection message is sent to initialize the manager.")
          stash()
        }
      } else {
        if (connectionIterator.hasNext) {
          val uri = connectionIterator.next()
          sender ! uri
        } else {
          sender ! Status.Failure(
            NoHostAvailableException(s"No connection is available. Current connections statuses $hostsStatus")
          )
        }
      }

    case status @ HostStatus(host, _) =>
      if (currentConfiguredHosts.contains(host)) {
        logHostStatus(status)
        hostsStatus.put(host, status)
        status.status match {
          case Alive   => connectionIterator.add(host)
          case Dead(_) => connectionIterator.remove(host)
        }
      } else {
        log.info(
          s"Received host status $status for host which is no longer enabled for this connection. Killing health check actor for it."
        )
        sender ! PoisonPill
        cleanUpHost(host)
      }
      if (!initialized) {
        initialized = true
        if (!fallbackToConfigurationHost) {
          log.info(s"Received first status. Unstashing all previous messages.")
          unstashAll()
        }
        log.info("Connection manager initialized")
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
      if (hostsStatus(host).status.code != status.status.code) {
        status.status match {
          case Alive =>
            log.info(s"Host ${status.host} is back online. Updating and reintroducing the host as viable connection.")
          case Dead(ex) =>
            log.info(s"Host ${status.host} is offline. Removing from viable connections because of exception.", ex)
        }
      }
    }
  }
}

object ConnectionManagerActor {

  def props(healthProvider: Uri => Props, config: Config): Props =
    Props(new ConnectionManagerActor(healthProvider, config))

  def healthCheckActorName(host: Uri) =
    s"${host.authority.host.address()}:${host.authority.port}"

  case class GetConnection()

  case class Connections(hosts: Set[Uri])

  case class NoHostAvailableException(msg: String) extends IllegalStateException(msg)
}
