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

  // Was a hardcoded 5 seconds, while ClusterAwareHostBalancer used `host-retrieval-timeout` -- one second by default
  // -- for the very same ask. Both now read the same setting.
  private implicit val timeout: Timeout =
    durationToTimeout(HostBalancer.hostRetrievalTimeout(system.settings.config))

  manager ! ConnectionManagerActor.Connections(hosts)

  override def nextHost: Future[Uri] = (manager ? GetConnection()).mapTo[Uri]
}
