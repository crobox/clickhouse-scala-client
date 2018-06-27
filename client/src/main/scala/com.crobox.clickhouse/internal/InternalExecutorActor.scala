package com.crobox.clickhouse.internal

import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import akka.pattern.pipe
import com.crobox.clickhouse.internal.InternalExecutorActor.{Execute, HealthCheck}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

class InternalExecutorActor(override protected val config: Config)
    extends Actor
    with ClickHouseExecutor
    with ClickhouseResponseParser
    with ClickhouseQueryBuilder {

  override implicit val system: ActorSystem = context.system

  override def receive = {
    case Execute(uri: Uri, query) =>
      val eventualResponse = executeRequest(Future.successful(uri), query)
      splitResponse(eventualResponse) pipeTo sender
    case HealthCheck(uri: Uri) =>
      val request = HttpRequest(method = HttpMethods.GET, uri = uri)
      splitResponse(handleResponse(singleRequest(request), "health check", uri)) pipeTo sender
  }

  private def splitResponse(eventualResponse: Future[String]) =
    eventualResponse.map(response => response.split("\n").toSeq)

  override protected lazy val bufferSize = 100
  override protected implicit val executionContext: ExecutionContext =
    context.dispatcher
}

object InternalExecutorActor {

  def props(config: Config) = Props(new InternalExecutorActor(config))

  case class Execute(host: Uri, query: String)
  case class HealthCheck(host: Uri)
}
