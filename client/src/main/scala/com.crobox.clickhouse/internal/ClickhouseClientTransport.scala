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
  * We expect to first find the request with the custom header and then receive the progress headers
  * The progress headers are being intercepted by the transport and sent to an internal source as progress events with the internal query id which will be used to route them to the query progress source
  * After the first progress header is received we send the end of headers mark so that akka can eagerly return the http response
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

  val ClickhouseProgressHeader = "X-ClickHouse-Progress"

  val in1                = Inlet[ByteString]("ProgressHeadersAsEvents.in1")
  val out1               = Outlet[ByteString]("ProgressHeadersAsEvents.out1")
  val in2                = Inlet[ByteString]("ProgressHeadersAsEvents.in2")
  val out2               = Outlet[ByteString]("ProgressHeadersAsEvents.out2")
  val Crlf               = "\r\n"
  val LookAheadCRLFSplit = "(?<=\\r\\n)"

  override val shape = BidiShape.of(in1, out1, in2, out2)
  override def createLogic(
      inheritedAttributes: Attributes
  ): GraphStageLogic = new GraphStageLogic(shape) with StageLogging {
    var queryId: Option[String] = None
    var queryIsInProgress       = false
    setHandler(
      in1,
      new InHandler {
        override def onPush(): Unit = {
          val byteString = grab(in1)
          if (byteString.containsSlice(ByteString(InternalQueryIdentifier))) {
            val incomingString  = byteString.utf8String
            val responseStrings = incomingString.split(Crlf)
            val queryIdHeader   = responseStrings.find(_.contains(InternalQueryIdentifier))
            if (queryIdHeader.isEmpty) {
              log.warning(s"Could not extract the query id from the containing $incomingString")
            }
            if (queryIsInProgress) {
              log.warning("The previous query was not terminated correctly")
            }
            queryId = queryIdHeader.map(header => header.stripPrefix(InternalQueryIdentifier + ":").trim)
          }
          push(out1, byteString)
        }
      }
    )
    setHandler(
      in2,
      new InHandler {
        override def onPush(): Unit = {
          val byteString = grab(in2)
          if (byteString.containsSlice(ByteString(ClickhouseProgressHeader))) {
            if (queryId.isEmpty) {
              log.warning("Cannot handle progress with query id")
            }
            val incomingString                  = byteString.utf8String
            val responseStrings                 = incomingString.split(LookAheadCRLFSplit)
            val (otherHeaders, progressHeaders) = responseStrings.span(!_.contains(ClickhouseProgressHeader))
            if (progressHeaders.isEmpty) {
              log.warning(s"Could not extract the progress from the containing $incomingString")
            }
            if (!queryIsInProgress) {
              queryIsInProgress = true
              val endOfHeaders = ByteString(otherHeaders.mkString("") + Crlf)
              push(out2, endOfHeaders)
            } else {
              push(out2, ByteString(otherHeaders.mkString("")))
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
          } else {
            if (queryIsInProgress) {
              queryIsInProgress = false
              if (byteString.equals(ByteString(Crlf))) { //already marked the end of headers, this must be removed
                pull(in2)
              } else {
                if (byteString.startsWith(Crlf)) {
                  push(out2, byteString.drop(2)) //already marked the end of headers, this must be removed
                } else {
                  push(out2, byteString)
                }
              }
            } else {
              push(out2, byteString)
            }
          }
        }
      }
    )
    setHandler(out1, new OutHandler {
      override def onPull(): Unit =
        pull(in1)
    })
    setHandler(out2, new OutHandler {
      override def onPull(): Unit =
        pull(in2)
    })
  }
}
