package com.crobox.clickhouse.internal

import akka.http.scaladsl.model.Uri

private[clickhouse] trait ClickhouseHostBuilder {

  def toHost(host: String, port: Int = 8123): Uri =
    if (host.startsWith("http:") || host.startsWith("https:")) {
      Uri(host).withPort(port)
    } else {
      Uri("http://" + host).withPort(port)
    }

}

private[clickhouse] object ClickhouseHostBuilder extends ClickhouseHostBuilder {}
