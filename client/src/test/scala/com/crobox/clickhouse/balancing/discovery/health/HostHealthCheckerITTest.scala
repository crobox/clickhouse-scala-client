package com.crobox.clickhouse.balancing.discovery.health

import akka.http.scaladsl.model.Uri
import akka.testkit.ImplicitSender
import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.Alive
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{HostStatus, IsAlive}
import com.crobox.clickhouse.internal.{ClickhouseHostBuilder, InternalExecutorActor}

import scala.concurrent.duration._

class HostHealthCheckerITTest extends ClickhouseClientSpec with ImplicitSender {

  private val host: Uri = ClickhouseHostBuilder
    .toHost("localhost", Some(8123))

  val checker = system.actorOf(
    HostHealthChecker.props(host, system.actorOf(InternalExecutorActor.props(config)), 5 seconds)
  )

  it should "return health" in {
    checker ! IsAlive()
    expectMsg(5 seconds, HostStatus(host, Alive))
  }
}
