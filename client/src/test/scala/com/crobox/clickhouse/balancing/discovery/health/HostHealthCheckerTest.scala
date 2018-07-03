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

  val checker = system.actorOf(
    HostHealthChecker.props(host, executorProbe.ref, 5 seconds)
  )
  it should "cache a health check" in {
    checker ! IsAlive()
    checker ! IsAlive()
    checker ! IsAlive()
    executorProbe.expectMsgPF() {
      case msg @ HealthCheck(`host`) =>
        executorProbe.reply(Seq("Ok."))
        msg
    }
    expectMsgAllOf(2 seconds, HostStatus(host, Alive), HostStatus(host, Alive), HostStatus(host, Alive))
    executorProbe.expectNoMessage()
  }

  it should "run new health check when completed" in {
    checker ! IsAlive()
    executorProbe.expectMsgPF() {
      case msg @ HealthCheck(`host`) =>
        executorProbe.reply(Seq("Ok."))
        msg
    }
    expectMsg(HostStatus(host, Alive))
    checker ! IsAlive()
    checker ! IsAlive()
    executorProbe.expectMsgPF() {
      case msg @ HealthCheck(`host`) =>
        executorProbe.reply(Seq("nok."))
        msg
    }
    expectMsgPF(1 second) {
      case HostStatus(`host`, _: Dead) =>
    }
    expectMsgPF(1 second) {
      case HostStatus(`host`, _: Dead) =>
    }
    executorProbe.expectNoMessage()
  }

}
