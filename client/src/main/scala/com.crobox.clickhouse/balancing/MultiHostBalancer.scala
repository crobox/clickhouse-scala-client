package com.crobox.clickhouse.balancing

import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import org.apache.pekko.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.GetConnection
import com.crobox.clickhouse.internal.ClickhouseHostBuilder

import scala.concurrent.Future
import scala.concurrent.duration._

case class MultiHostBalancer(hosts: Set[Uri], manager: ActorRef)(implicit system: ActorSystem)
    extends HostBalancer
    with ClickhouseHostBuilder {

  private implicit val timeout: Timeout = durationToTimeout(5.seconds)

  manager ! ConnectionManagerActor.Connections(hosts)

  override def nextHost: Future[Uri] = (manager ? GetConnection()).mapTo[Uri]
}
