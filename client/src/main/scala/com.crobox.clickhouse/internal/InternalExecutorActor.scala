package com.crobox.clickhouse.internal

import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.model.Uri
import akka.pattern.pipe
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.internal.ClickHouseExecutor.QuerySettings
import com.crobox.clickhouse.internal.ClickHouseExecutor.QuerySettings.ReadQueries
import com.crobox.clickhouse.internal.InternalExecutorActor.Execute
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

class InternalExecutorActor(override protected val config: Config)(implicit val executionContext: ExecutionContext)
    extends Actor
    with ClickHouseExecutor
    with ClickhouseResponseParser
    with ClickhouseQueryBuilder {

  override implicit val system: ActorSystem         = context.system
  override protected val hostBalancer: HostBalancer = null

  override def receive = {
    case Execute(uri: Uri, query) =>
      val eventualResponse =
        executeRequestInternal(Future.successful(uri),
                               query,
                               "internal",
                               QuerySettings(ReadQueries).withFallback(config),
                               None,
                               None)
      eventualResponse.map(splitResponse) pipeTo sender
  }

  override protected lazy val bufferSize = 100

}

object InternalExecutorActor {

  def props(config: Config)(implicit ec: ExecutionContext) = Props(new InternalExecutorActor(config))

  case class Execute(host: Uri, query: String)
}
