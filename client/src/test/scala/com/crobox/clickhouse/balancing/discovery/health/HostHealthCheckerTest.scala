package com.crobox.clickhouse.balancing.discovery.health

import akka.http.scaladsl.model.Uri
import akka.testkit.ImplicitSender
import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.Alive
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{HostStatus, IsAlive}
import com.crobox.clickhouse.internal.{ClickhouseHostBuilder, InternalExecutorActor}

import scala.concurrent.duration._

class HostHealthCheckerTest extends ClickhouseClientSpec with ImplicitSender {

  private val host: Uri = ClickhouseHostBuilder
    .toHost("localhost", Some(8123))

  val checker = system.actorOf(
    HostHealthChecker.props(host, system.actorOf(InternalExecutorActor.props(config)), 2 seconds)
  )

  it should "return health" in {
    checker ! IsAlive()
    expectMsg(HostStatus(host, Alive))
  }

}
