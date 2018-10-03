package com.crobox.clickhouse.internal

import akka.actor.ActorSystem
import akka.http.scaladsl.{ClientTransport, Http}
import akka.http.scaladsl.settings.ClientConnectionSettings
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import akka.stream.scaladsl.{BidiFlow, Flow, SourceQueue}
import akka.stream.stage._
import akka.util.ByteString
import com.crobox.clickhouse.internal.ClickHouseExecutor.InternalQueryIdentifier

import scala.concurrent.Future

/**
 * Clickhouse sends http progress headers with the name X-ClickHouse-Progress which cannot be handled in a streaming way in akka
 * In the request we include our own custom header `X-Internal-Identifier` so we can send the internal query id with the progress
 * The progress headers are being intercepted by the transport and sent to an internal source as progress events with the internal query id which will be used to route them to the query progress source
 * We just proxy the request/response and do not manipulate them in any way
 * */
class ProgressHeadersAsBodyClientTransport(source: SourceQueue[String]) extends ClientTransport {
  override def connectTo(
      host: String,
      port: Int,
      settings: ClientConnectionSettings
  )(implicit system: ActorSystem): Flow[
    ByteString,
    ByteString,
    Future[Http.OutgoingConnection]
  ] =
    BidiFlow
      .fromGraph(new ProgressHeadersAsEventsStage(source))
      .joinMat(
        ClientTransport.TCP
          .connectTo(host, port, settings)
      )((_, result) => result)
}

class ProgressHeadersAsEventsStage(source: SourceQueue[String])
    extends GraphStage[BidiShape[ByteString, ByteString, ByteString, ByteString]] {
  import ProgressHeadersAsEventsStage._
  private val clientInput  = Inlet[ByteString]("ProgressHeadersAsEvents.in1")
  private val serverOutput = Outlet[ByteString]("ProgressHeadersAsEvents.out1")
  private val serverInput  = Inlet[ByteString]("ProgressHeadersAsEvents.in2")
  private val clientOutput = Outlet[ByteString]("ProgressHeadersAsEvents.out2")

  override val shape = BidiShape.of(clientInput, serverOutput, serverInput, clientOutput)
  override def createLogic(
      inheritedAttributes: Attributes
  ): GraphStageLogic = new GraphStageLogic(shape) with StageLogging {
    var queryId: Option[String] = None
    var queryMarkedAsAccepted   = false
    setHandler(
      clientInput,
      new InHandler {
        override def onPush(): Unit = {
          val byteString = grab(clientInput)
          if (byteString.containsSlice(ByteString(InternalQueryIdentifier))) {
            val incomingString  = byteString.utf8String
            val responseStrings = incomingString.split(Crlf)
            val queryIdHeader   = responseStrings.find(_.contains(InternalQueryIdentifier))
            if (queryIdHeader.isEmpty) {
              log.warning(s"Could not extract the query id from the containing $incomingString")
            }
            queryId = queryIdHeader.map(header => {
              queryMarkedAsAccepted = false
              header.stripPrefix(InternalQueryIdentifier + ":").trim
            })
          }
          push(serverOutput, byteString)
        }
      }
    )
    setHandler(
      serverInput,
      new InHandler {
        override def onPush(): Unit = {
          val byteString = grab(serverInput)
          push(clientOutput, byteString)
          if (!queryMarkedAsAccepted && byteString.containsSlice(ByteString("HTTP/1.1 200 OK"))) {
            source.offer(queryId.getOrElse("unknown") + "\n" + AcceptedMark)
            queryMarkedAsAccepted = true
          }
          if (byteString.containsSlice(ByteString(ClickhouseProgressHeader))) {
            if (queryId.isEmpty) {
              log.warning("Cannot handle progress with query id")
            } else {
              val incomingString  = byteString.utf8String
              val responseStrings = incomingString.split(Crlf)
              val progressHeaders = responseStrings.filter(_.contains(ClickhouseProgressHeader))
              if (progressHeaders.isEmpty) {
                log.warning(s"Could not extract the progress from the containing $incomingString")
              }
              progressHeaders
                .filter(_.contains(ClickhouseProgressHeader))
                .map(_.stripPrefix(ClickhouseProgressHeader + ":"))
                .map(progressJson => {
                  queryId.getOrElse("unknown") + "\n" + progressJson
                })
                .foreach(progress => {
                  source.offer(progress)
                })
            }
          }
        }
      }
    )
    setHandler(serverOutput, new OutHandler {
      override def onPull(): Unit =
        pull(clientInput)
    })
    setHandler(clientOutput, new OutHandler {
      override def onPull(): Unit =
        pull(serverInput)
    })
  }
}

object ProgressHeadersAsEventsStage {

  val ClickhouseProgressHeader = "X-ClickHouse-Progress"
  val AcceptedMark             = "CLICKHOUSE_ACCEPTED"
  val Crlf                     = "\r\n"

}
