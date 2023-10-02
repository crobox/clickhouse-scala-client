package com.crobox.clickhouse.balancing.discovery.health

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.{ActorSystem, Cancellable}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshaller
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import com.crobox.clickhouse.internal.ClickhouseResponseParser

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object ClickhouseHostHealth extends ClickhouseResponseParser {

  sealed trait ClickhouseHostStatus {
    val host: Uri
    val code: String
  }

  case class Alive(host: Uri) extends ClickhouseHostStatus { override val code: String = "ok" }

  case class Dead(host: Uri, reason: Throwable) extends ClickhouseHostStatus { override val code: String = "nok" }

  /**
   * Creates a source which emits the health status at most every `health-check.interval` interval.
   * The source uses a cachedHostConnectionPool with a number of one maximum connections and one maximum open requests. This is configured on
   * the provided actor system and assumes there is no other user of such a pool, so it will not be shared.
   * This ensures the health checks will not affect the clients `superPool`  in any way, and it will not fill the queue if one hosts hangs when returning the response.
   * We also set the connection idle timeout to `health-check.timeout + health-check.interval` to ensure that the pool will be blocked with on hanging request.
   * */
  def healthFlow(host: Uri)(
      implicit system: ActorSystem,
      executionContext: ExecutionContext
  ): Source[ClickhouseHostStatus, Cancellable] = {
    val healthCheckInterval: FiniteDuration =
      system.settings.config
        .getDuration("connection.health-check.interval")
        .getSeconds
        .seconds
    val healthCheckTimeout: FiniteDuration =
      system.settings.config
        .getDuration("connection.health-check.timeout")
        .getSeconds
        .seconds

    val healthCachedPool = Http(system).cachedHostConnectionPool[Int](
      host.authority.host.address(),
      host.effectivePort,
      settings = ConnectionPoolSettings(system)
        .withMaxConnections(1)
        .withMinConnections(1)
        .withMaxOpenRequests(2)
        .withMaxRetries(3)
        .withUpdatedConnectionSettings(
          _.withIdleTimeout(healthCheckTimeout + healthCheckInterval).withConnectingTimeout(healthCheckTimeout)
        )
    )
    Source
      .tick(0.milliseconds, healthCheckInterval, 0)
      .map(tick => (HttpRequest(method = HttpMethods.GET, uri = host), tick))
      .via(healthCachedPool)
      .via(parsingFlow(host))
  }

  private[health] def parsingFlow[T](
      host: Uri
  )(implicit ec: ExecutionContext, mat: Materializer): Flow[(Try[HttpResponse], T), ClickhouseHostStatus, NotUsed] =
    Flow[(Try[HttpResponse], T)].mapAsync(1) {
      case (Success(response @ org.apache.pekko.http.scaladsl.model.HttpResponse(StatusCodes.OK, _, _, _)), _) =>
        Unmarshaller
          .stringUnmarshaller(decodeResponse(response).entity)
          .map(splitResponse)
          .map(
            stringResponse =>
              if (stringResponse.equals(Seq("Ok."))) {
                Alive(host)
              } else {
                Dead(host, new IllegalArgumentException(s"Got wrong result $stringResponse"))
            }
          )
      case (Success(response), _) =>
        Future.successful(Dead(host, new IllegalArgumentException(s"Got response with status code ${response.status}")))
      case (Failure(ex), _) =>
        Future.successful(Dead(host, ex))
    }
}
