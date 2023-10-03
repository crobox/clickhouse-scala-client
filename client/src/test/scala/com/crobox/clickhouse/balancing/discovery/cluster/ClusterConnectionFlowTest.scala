package com.crobox.clickhouse.balancing.discovery.cluster

import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import com.crobox.clickhouse.ClickhouseClientAsyncSpec
import com.crobox.clickhouse.internal.ClickhouseHostBuilder

import scala.concurrent._
import scala.concurrent.duration._

class ClusterConnectionFlowTest extends ClickhouseClientAsyncSpec {

  private val clickhouseUri: Uri = ClickhouseHostBuilder.toHost("localhost", Some(8123))
  it should "select cluster hosts" in {
    val (_, futureResult) = ClusterConnectionFlow
      .clusterConnectionsFlow(Future.successful(clickhouseUri), 2.seconds, "test_shard_localhost")
      .toMat(Sink.head)(Keep.both)
      .run()
    futureResult.map(result => {
      result.hosts should contain only ClickhouseHostBuilder.toHost("127.0.0.1", Some(8123))
    })
  }

  it should "fail for non existing cluster" in {
    val (_, futureResult) = ClusterConnectionFlow
      .clusterConnectionsFlow(Future.successful(clickhouseUri), 2.seconds, "cluster")
      .toMat(Sink.head)(Keep.both)
      .run()
    futureResult
      .map(_ => {
        fail("Returned answer for non existing clsuter")
      })
      .recover {
        case _: IllegalArgumentException => succeed
      }
  }

}
