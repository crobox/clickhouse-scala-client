package com.crobox.clickhouse.balancing

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.Uri
import akka.pattern.ask
import akka.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.GetConnection
import com.crobox.clickhouse.balancing.discovery.cluster.ClusterConnectionProviderActor.ScanHosts
import com.crobox.clickhouse.discovery.ConnectionConfig

import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Host balancer that does a round robin on all the entries found in the `system.clusters` table.
  * It assumes that the service itself can access directly the clickhouse nodes and that the default port `8123` is used
  * for every node.
  **/
case class ClusterAwareHostBalancer(host: Uri,
                                    cluster: String = "cluster", manager: ActorRef, provider: ActorRef)(implicit val system: ActorSystem)
  extends HostBalancer {
  private implicit val timeout = durationToTimeout(1 second)

  import system.dispatcher

  system.scheduler.schedule(Duration.Zero,
    10 seconds,
    provider,
    ScanHosts(ConnectionConfig(host, cluster)))

  override def nextHost: Future[Uri] = {
    (manager ? GetConnection()).mapTo[Uri]
  }
}
