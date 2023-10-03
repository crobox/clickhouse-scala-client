package com.crobox.clickhouse.balancing.discovery

import org.apache.pekko.actor.{Actor, ActorLogging, Cancellable, PoisonPill, Props, Stash, Status}
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.balancing.discovery.health.ClickhouseHostHealth.{Alive, ClickhouseHostStatus, Dead}
import com.crobox.clickhouse.balancing.iterator.CircularIteratorSet
import com.typesafe.config.Config

import scala.collection.mutable
import scala.concurrent.duration._

class ConnectionManagerActor(healthSource: Uri => Source[ClickhouseHostStatus, Cancellable],
                             optionalConfig: Option[Config])(
    implicit materializer: Materializer
) extends Actor
    with ActorLogging
    with Stash {

  import ConnectionManagerActor._

  private val config                      = optionalConfig.getOrElse(context.system.settings.config).getConfig("connection")
  private val fallbackToConfigurationHost = config.getBoolean("fallback-to-config-host-during-initialization")

  //  state
  private val connectionIterator: CircularIteratorSet[Uri]        = new CircularIteratorSet[Uri]()
  private val hostsStatus: mutable.Map[Uri, ClickhouseHostStatus] = mutable.Map.empty
  private val hostHealthScheduler: mutable.Map[Uri, Cancellable]  = mutable.Map.empty
  private var currentConfiguredHosts: Set[Uri]                    = Set.empty
  private var initialized: Boolean                                = false

  context.system.scheduler.scheduleWithFixedDelay(30.seconds, 30.seconds, self, LogDeadConnections)(
    context.system.dispatcher
  )

  override def receive: Receive = {
    case Connections(hosts) =>
      hosts
        .foreach(host => {
          if (!currentConfiguredHosts.contains(host)) {
            log.info(s"Setting up host health checks for host $host")
            hostHealthScheduler.put(
              host,
              healthSource(host)
                .toMat(
                  Sink.actorRef(self, LogDeadConnections, throwable => log.error(throwable.getMessage, throwable))
                )(Keep.left)
                .run()
            )
          }
        })
      currentConfiguredHosts = hosts

    case GetConnection() =>
      if (!initialized) {
        if (fallbackToConfigurationHost) {
          log.warning("Not yet initialized, returning the config host.")
          sender() ! HostBalancer.extractHost(config)
        } else {
          log.warning("Stashing get connection message until connection message is sent to initialize the manager.")
          stash()
        }
      } else {
        if (connectionIterator.hasNext) {
          val uri = connectionIterator.next()
          sender() ! uri
        } else {
          sender() ! Status.Failure(
            NoHostAvailableException(s"No connection is available. Current connections statuses $hostsStatus")
          )
        }
      }

    case status: ClickhouseHostStatus =>
      val host = status.host
      if (currentConfiguredHosts.contains(host)) {
        logHostStatus(status)
        hostsStatus.put(host, status)
        status match {
          case _: Alive => connectionIterator.add(host)
          case _: Dead  => connectionIterator.remove(host)
        }
      } else {
        log.info(
          s"Received host status $status for host which is no longer enabled for this connection. Killing health check actor for it."
        )
        sender() ! PoisonPill
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

    case LogDeadConnections =>
      val deadHosts = hostsStatus.values.collect {
        case Dead(host, _) => host
      }
      if (deadHosts.nonEmpty)
        log.error(s"Hosts ${deadHosts.mkString(" - ")} are still unreachable")
  }

  private def cleanUpHost(host: Uri) = {
    hostsStatus.remove(host)
    connectionIterator.remove(host)
    hostHealthScheduler.get(host).foreach(_.cancel())
    hostHealthScheduler.remove(host)
  }

  private def logHostStatus(status: ClickhouseHostStatus): Unit = {
    val host = status.host
    if (!hostsStatus.contains(host)) {
      log.info(s"Adding host status $status")
    } else {
      if (hostsStatus(host).code != status.code) {
        status match {
          case _: Alive =>
            log.info(s"Host ${status.host} is back online. Updating and reintroducing the host as viable connection.")
          case Dead(_, ex) =>
            log.error(ex, s"Host ${status.host} is offline. Removing from viable connections because of exception.")
        }
      }
    }
  }
}

object ConnectionManagerActor {

  def props(healthProvider: Uri => Source[ClickhouseHostStatus, Cancellable], optionalConfig: Option[Config] = None)(
      implicit materializer: Materializer
  ): Props = Props(new ConnectionManagerActor(healthProvider, optionalConfig))

  def healthCheckActorName(host: Uri) =
    s"${host.authority.host.address()}:${host.authority.port}"

  case class GetConnection()

  case class Connections(hosts: Set[Uri])
  case class NoHostAvailableException(msg: String) extends IllegalStateException(msg)
  private[balancing] case object LogDeadConnections
}
