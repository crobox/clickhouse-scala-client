package com.crobox.clickhouse.internal.progress
import akka.NotUsed
import akka.stream.scaladsl.{BroadcastHub, Keep, RunnableGraph, Source, SourceQueueWithComplete}
import akka.stream.{ActorAttributes, OverflowStrategy, Supervision}
import com.typesafe.scalalogging.LazyLogging
import spray.json._
import spray.json.DefaultJsonProtocol._
import scala.util.{Failure, Success, Try}

object QueryProgress extends LazyLogging {

  sealed trait QueryProgress
  case object QueryAccepted                                 extends QueryProgress
  case object QueryFinished                                 extends QueryProgress
  case object QueryRejected                                 extends QueryProgress
  case class QueryFailed(cause: Throwable)                  extends QueryProgress
  case class QueryRetry(cause: Throwable, retryNumber: Int) extends QueryProgress

  case class ClickhouseQueryProgress(identifier: String, progress: QueryProgress)
  case class Progress(rowsRead: Long, bytesRead: Long, totalRows: Long) extends QueryProgress

  def queryProgressStream: RunnableGraph[(SourceQueueWithComplete[String], Source[ClickhouseQueryProgress, NotUsed])] =
    Source
      .queue[String](1000, OverflowStrategy.dropHead)
      .map[Option[ClickhouseQueryProgress]](queryAndProgress => {
        queryAndProgress.split("\n", 2).toList match {
          case queryId :: ProgressHeadersAsEventsStage.AcceptedMark :: Nil =>
            Some(ClickhouseQueryProgress(queryId, QueryAccepted))
          case queryId :: progressJson :: Nil =>
            Try {
              progressJson.parseJson match {
                case JsObject(fields) if fields.size == 3 =>
                  ClickhouseQueryProgress(
                    queryId,
                    Progress(
                        fields("read_rows").convertTo[Long],
                        fields("read_bytes").convertTo[Long],
                        fields("total_rows").convertTo[Long]
                    )
                  )
                case _ => throw new IllegalArgumentException(s"Cannot extract progress from $progressJson")
              }
            } match {
              case Success(value) => Some(value)
              case Failure(exception) =>
                logger.warn(s"Failed to parse json $progressJson", exception)
                None
            }
          case other @ _ =>
            logger.warn(s"Could not get progress from $other")
            None

        }
      })
      .collect {
        case Some(progress) => progress
      }
      .withAttributes(ActorAttributes.supervisionStrategy({
        case ex @ _ =>
          logger.warn("Detected failure in the query progress stream, resuming operation.", ex)
          Supervision.Resume
      }))
      .toMat(BroadcastHub.sink)(Keep.both)
}
