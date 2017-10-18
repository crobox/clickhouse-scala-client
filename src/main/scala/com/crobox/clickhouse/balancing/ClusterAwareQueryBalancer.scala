package com.crobox.clickhouse.balancing

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.{ActorMaterializer, Materializer}
import com.crobox.clickhouse.ClickHouseExecutor

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Host balancer that does a round robin on all the entries found in the `system.clusters` table.
  * It assumes that the service itself can access directly the clickhouse nodes and that the default port `8123` is used
  * for every node.
  **/
case class ClusterAwareQueryBalancer(host: String, cluster: String = "cluster")(implicit val system: ActorSystem) extends QueryBalancer with ClickHouseExecutor {
  override implicit val materializer: Materializer = ActorMaterializer()

  import system.dispatcher

  override val bufferSize = 1
  private val hosts: Seq[Uri] = {
    val originalUriHost = toHost(host)
    Await.result(executeRequest(originalUriHost, s"SELECT host_address FROM system.clusters WHERE cluster='$cluster'").map(result => {
      if (result.isEmpty) {
        logger.warn("Could not determine hosts from clusters table. Default to single host.")
        Seq(originalUriHost)
      } else {
        val parsedHosts = result.split("\n").map(toHost)
        logger.info(s"Got hosts [${parsedHosts.map(_.toString()).mkString(",")}]")
        parsedHosts
      }
    }), 10 seconds)
  }

  private val hostIterator = Iterator.continually(hosts).flatten

  override def nextHost: Uri = {
    hostIterator.next()
  }
}
