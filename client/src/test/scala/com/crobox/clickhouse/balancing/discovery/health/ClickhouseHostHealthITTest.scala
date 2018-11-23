package com.crobox.clickhouse.balancing.discovery.health

import akka.http.scaladsl.model.Uri
import akka.stream.scaladsl.{Keep, Sink}
import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.balancing.discovery.health.ClickhouseHostHealth.Alive
import com.crobox.clickhouse.internal.ClickhouseHostBuilder

class ClickhouseHostHealthITTest extends ClickhouseClientSpec {

  private val host: Uri = ClickhouseHostBuilder
    .toHost("localhost", Some(8123))

  it should "return health" in {
    val result = ClickhouseHostHealth.healthFlow(host).toMat(Sink.head)(Keep.right).run()
    result.futureValue should be(Alive(host))
  }
}
