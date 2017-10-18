package com.crobox.clickhouse.balancing

import akka.http.scaladsl.model._
import com.typesafe.scalalogging.LazyLogging

trait QueryBalancer extends
  LazyLogging {

  def nextHost: Uri

  protected def toHost(host: String): Uri = if (host.startsWith("http:")) {
    Uri(host)
  } else {
    Uri("http://" + host).withPort(8123)
  }

}