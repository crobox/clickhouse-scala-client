package com.crobox.clickhouse.balancing

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, Materializer}
import com.crobox.clickhouse.ClickHouseExecutor
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Await
import scala.concurrent.duration._

trait QueryBalancer extends
  LazyLogging {

  def nextHost: Uri

  protected def toHost(host: String): Uri = if (host.startsWith("http:")) {
    Uri(host)
  } else {
    Uri("http://" + host).withPort(8123)
  }

}
case class SingleHostQueryBalancer(host: String) extends QueryBalancer {

  private val hostUri: Uri = toHost(host)

  override def nextHost: Uri = hostUri
}
case class ClusterAwareQueryBalancer(host: String)(implicit val system: ActorSystem) extends QueryBalancer with ClickHouseExecutor {
  override implicit val materializer: Materializer = ActorMaterializer()

  import system.dispatcher

  override val bufferSize = 1
  private val hosts: Seq[Uri] = {
    val originalUriHost = toHost(host)
    Await.result(executeRequest(originalUriHost, "SELECT host_address FROM system.clusters").map(result => {
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