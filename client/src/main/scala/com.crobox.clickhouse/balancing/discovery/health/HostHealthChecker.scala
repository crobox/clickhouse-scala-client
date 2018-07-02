package com.crobox.clickhouse.balancing.discovery.health

import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

import akka.actor.{Actor, ActorRef, Props}
import akka.http.scaladsl.model.Uri
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import akka.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.{Alive, Dead}
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{HostStatus, IsAlive}
import com.crobox.clickhouse.internal.InternalExecutorActor.HealthCheck

import scala.concurrent.Future
import scala.concurrent.duration._

class HostHealthChecker(host: Uri, executor: ActorRef)(implicit val timeout: Timeout) extends Actor {

  import context.dispatcher

  private val currentCheck = new AtomicReference[Future[HostStatus]](doHostCheck)

  override def receive = {
    case IsAlive() =>
      currentCheck.updateAndGet(new UnaryOperator[Future[HostStatus]] {
        override def apply(current: Future[HostStatus]): Future[HostStatus] = {
          if (current.isCompleted) {
            doHostCheck
          } else {
            current
          }
        }
      }) pipeTo sender

  }

  private def doHostCheck() =
    (executor ? HealthCheck(host))
      .mapTo[Seq[String]]
      .map(result => {
        if (result.equals(Seq("Ok."))) {
          HostStatus(host, Alive)
        } else {
          HostStatus(host, Dead(new IllegalArgumentException(s"Got wrong result $result")))
        }
      })
      .recover {
        case ex: Throwable =>
          HostStatus(host, Dead(ex))
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
