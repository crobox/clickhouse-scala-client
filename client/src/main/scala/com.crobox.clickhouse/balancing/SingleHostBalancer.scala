package com.crobox.clickhouse.balancing

import org.apache.pekko.http.scaladsl.model.Uri

import scala.concurrent.Future

/**
 * The default host balancer which always provides the same host.
  **/
case class SingleHostBalancer(host: Uri) extends HostBalancer {

  override def nextHost: Future[Uri] = Future.successful(host)
}
