package com.crobox.clickhouse.balancing.discovery.health

import org.apache.pekko.http.scaladsl.model.{HttpResponse, StatusCodes}
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.balancing.discovery.health.ClickhouseHostHealth.{Alive, Dead}
import com.crobox.clickhouse.internal.ClickhouseHostBuilder

import scala.util.{Failure, Success}

class ClickhouseHostHealthTest extends ClickhouseClientSpec {

  private val host = ClickhouseHostBuilder.toHost("localhost", Some(8123))
  it should "emit alive for ok response" in {
    Source
      .single((Success(HttpResponse(entity = "Ok.")), 0))
      .via(ClickhouseHostHealth.parsingFlow(host))
      .runWith(Sink.seq)
      .futureValue should contain theSameElementsAs Seq(Alive(host))
  }

  it should "emit dead for response with other status code" in {
    Source
      .single((Success(HttpResponse(status = StatusCodes.InternalServerError)), 0))
      .via(ClickhouseHostHealth.parsingFlow(host))
      .runWith(Sink.seq)
      .futureValue
      .toList match {
      case Dead(`host`, _) :: Nil => succeed
      case _                      => fail()
    }
  }

  it should "emit dead for failure" in {
    Source
      .single((Failure(new IllegalArgumentException), 0))
      .via(ClickhouseHostHealth.parsingFlow(host))
      .runWith(Sink.seq)
      .futureValue
      .toList match {
      case Dead(`host`, _) :: Nil => succeed
      case _                      => fail()
    }
  }

}
