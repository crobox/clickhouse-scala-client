package com.crobox.clickhouse.internal

import akka.actor.{Actor, ActorSystem, Props}
import akka.http.scaladsl.model.Uri
import com.crobox.clickhouse.internal.InternalExecutorActor.Execute
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

class InternalExecutorActor(override protected val config: Config)
    extends Actor
    with ClickHouseExecutor
    with ClickhouseResponseParser
    with ClickhouseQueryBuilder {

  override implicit val system: ActorSystem = context.system

  override def receive = {
    case Execute(uri: Uri, query: String) =>
      executeRequest(Future.successful(uri), query)
  }

  override protected lazy val bufferSize = 100
  override protected implicit val executionContext: ExecutionContext =
    context.dispatcher
}
object InternalExecutorActor {
  def props(config: Config) = Props(new InternalExecutorActor(config))
  case class Execute(host: Uri, query: String)
}
