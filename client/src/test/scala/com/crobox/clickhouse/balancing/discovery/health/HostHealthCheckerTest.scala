package com.crobox.clickhouse.balancing.discovery.health

import akka.http.scaladsl.model.Uri
import akka.testkit.{ImplicitSender, TestProbe}
import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.{Alive, Dead}
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{HostStatus, IsAlive}
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.crobox.clickhouse.internal.InternalExecutorActor.HealthCheck

import scala.concurrent.duration._

class HostHealthCheckerTest extends ClickhouseClientSpec with ImplicitSender {
  private val host: Uri = ClickhouseHostBuilder
    .toHost("host", None)
  val executorProbe = TestProbe()

  private val duration: FiniteDuration = 5 seconds
  val checker = system.actorOf(
    HostHealthChecker.props(host, executorProbe.ref, duration)
  )
  it should "cache a health check" in {
    checker ! IsAlive()
    checker ! IsAlive()
    checker ! IsAlive()
    executorProbe.expectMsgPF(max = duration) {
      case msg @ HealthCheck(`host`) =>
        executorProbe.reply(Seq("Ok."))
        msg
    }
    expectMsgAllOf(duration, HostStatus(host, Alive), HostStatus(host, Alive), HostStatus(host, Alive))
    executorProbe.expectNoMessage()
  }

  it should "run new health check when completed" in {
    checker ! IsAlive()
    executorProbe.expectMsgPF(duration) {
      case msg @ HealthCheck(`host`) =>
        executorProbe.reply(Seq("Ok."))
        msg
    }
    expectMsg(duration, HostStatus(host, Alive))
    checker ! IsAlive()
    checker ! IsAlive()
    executorProbe.expectMsgPF(duration) {
      case msg @ HealthCheck(`host`) =>
        executorProbe.reply(Seq("nok."))
        msg
    }
    expectMsgPF(duration) {
      case HostStatus(`host`, _: Dead) =>
    }
    expectMsgPF(duration) {
      case HostStatus(`host`, _: Dead) =>
    }
    executorProbe.expectNoMessage()
  }

}
