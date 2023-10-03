package com.crobox.clickhouse.balancing

import org.apache.pekko.http.scaladsl.model.Uri
import com.crobox.clickhouse.ClickhouseClientAsyncSpec

import scala.concurrent.Future

class SingleHostBalancerTest extends ClickhouseClientAsyncSpec {

  it should "return the same host every time" in {
    val uri      = Uri("localhost").withPort(8123)
    val balancer = SingleHostBalancer(uri)
    val assertions = (1 to 10)
      .map(_ => {
        balancer.nextHost.map(_ shouldEqual uri)
      })
    Future.sequence(assertions).map(_ => succeed)
  }

}
