package com.crobox.clickhouse.internal

import org.apache.pekko.http.scaladsl.model.Uri

private[clickhouse] trait ClickhouseHostBuilder {

  def toHost(host: String, port: Option[Int]): Uri =
    if (host.startsWith("http:") || host.startsWith("https:")) {
      val uri = Uri(host)
      port.map(uri.withPort).getOrElse(uri)
    } else {
      val uri = Uri("http://" + host)
      port.map(uri.withPort).getOrElse(uri)
    }

}

private[clickhouse] object ClickhouseHostBuilder extends ClickhouseHostBuilder {}
