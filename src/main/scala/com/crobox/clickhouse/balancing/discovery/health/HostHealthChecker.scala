package com.crobox.clickhouse.balancing.discovery.health

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model.Uri
import akka.pattern.{ask, pipe}
import akka.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.{
  Alive,
  Dead
}
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{
  HostStatus,
  IsAlive
}
import com.crobox.clickhouse.internal.InternalExecutorActor.Execute
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
class HostHealthChecker(host: Uri, executor: ActorRef)
    extends Actor
    with LazyLogging {
  private implicit val timeout = durationToTimeout(5 second)

  import context.dispatcher
  override def receive = {
    case IsAlive() =>
      (executor ? Execute(host, "SELECT 1"))
        .mapTo[Seq[String]]
        .map(result => {
          if (result.equals(Seq("1"))) {
            logger.trace(s"Host is alive for host ${host.toString()}")
            HostStatus(host, Alive)
          } else {
            logger.warn(
              s"Host ${host.toString()} status is DEAD because of response $result")
            HostStatus(host, Dead)
          }
        })
        .recover {
          case ex: Throwable =>
            logger.error(
              s"Host ${host.toString()} status is DEAD because of exception",
              ex)
            HostStatus(host, Dead)
        } pipeTo sender
  }
}
object HostHealthChecker {
  def props(host: Uri, executor: ActorRef) =
    Props(new HostHealthChecker(host, executor))
  case class IsAlive()
  trait Status
  object Status {
    case object Alive extends Status
    case object Dead extends Status
  }

  case class HostStatus(host: Uri, status: Status)
}
