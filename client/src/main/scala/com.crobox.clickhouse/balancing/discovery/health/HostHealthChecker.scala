package com.crobox.clickhouse.balancing.discovery.health

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model.Uri
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import akka.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.{Alive, Dead}
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{HostStatus, IsAlive}
import com.crobox.clickhouse.internal.InternalExecutorActor.{Execute, HealthCheck}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._

class HostHealthChecker(host: Uri, executor: ActorRef)(implicit val timeout: Timeout) extends Actor with LazyLogging {

  import context.dispatcher

  override def receive = {
    case IsAlive() =>
      (executor ? HealthCheck(host))
        .mapTo[Seq[String]]
        .map(result => {
          if (result.equals(Seq("Ok."))) {
            logger.trace(s"Host is alive for host ${host.toString()}")
            HostStatus(host, Alive)
          } else {
            logger.warn(s"Host ${host.toString()} status is DEAD because of response $result")
            HostStatus(host, Dead(new IllegalArgumentException(s"Got wrong result $result")))
          }
        })
        .recover {
          case ex: Throwable =>
            HostStatus(host, Dead(ex))
        } pipeTo sender
  }
}

object HostHealthChecker {

  def props(host: Uri, executor: ActorRef, timeout: FiniteDuration) =
    Props(new HostHealthChecker(host, executor)(durationToTimeout(timeout)))

  case class IsAlive()

  sealed trait Status {
    val code: String
  }

  object Status {

    case object Alive extends Status {
      override val code: String = "ok"
    }

    case class Dead(reason: Throwable) extends Status {
      override val code: String = "nok"
    }
  }

  case class HostStatus(host: Uri, status: Status)
}
