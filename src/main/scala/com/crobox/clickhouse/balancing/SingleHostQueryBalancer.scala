package com.crobox.clickhouse.balancing

import akka.http.scaladsl.model.Uri

/**
  * The default host balancer which always provides the same host.
  * */
case class SingleHostQueryBalancer(host: String) extends QueryBalancer {

  private val hostUri: Uri = toHost(host)

  override def nextHost: Uri = hostUri
}
