package com.crobox.clickhouse.balancing.discovery.cluster

import org.apache.pekko.actor.{ActorSystem, Cancellable}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.stream.scaladsl.Source
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.Connections
import com.crobox.clickhouse.internal.QuerySettings.ReadQueries
import com.crobox.clickhouse.internal.{
  ClickhouseHostBuilder,
  ClickhouseQueryBuilder,
  ClickhouseResponseParser,
  QuerySettings
}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

private[clickhouse] object ClusterConnectionFlow
    extends ClickhouseQueryBuilder
    with ClickhouseResponseParser
    with LazyLogging {

  def clusterConnectionsFlow(
      targetHost: => Future[Uri],
      scanningInterval: FiniteDuration,
      cluster: String
  )(implicit system: ActorSystem, ec: ExecutionContext): Source[Connections, Cancellable] = {
    val http = Http(system)
    val settings = ConnectionPoolSettings(system)
      .withMaxConnections(1)
      .withMinConnections(1)
      .withMaxOpenRequests(2)
      .withMaxRetries(3)
      .withUpdatedConnectionSettings(_.withIdleTimeout(scanningInterval.plus(1.second)))
    Source
      .tick(0.millis, scanningInterval, {})
      .mapAsync(1)(_ => targetHost)
      .mapAsync(1)(host => {
        val query = s"SELECT host_address FROM system.clusters WHERE cluster='$cluster'"
        val request =
          toRequest(host, query, None, QuerySettings(readOnly = ReadQueries, idempotent = Some(true)), None)(
            system.settings.config
          )
        processClickhouseResponse(http.singleRequest(request, settings = settings), query, host, None)
          .map(splitResponse)
          .map(_.toSet.filter(_.nonEmpty))
          .map(result => {
            if (result.isEmpty) {
              throw new IllegalArgumentException(
                s"Could not determine clickhouse cluster hosts for cluster $cluster and host $host. " +
                s"This could indicate that you are trying to use the cluster balancer to connect to a non cluster based clickhouse server. " +
                s"Please use the `SingleHostQueryBalancer` in that case."
              )
            }
            Connections(result.map(ClickhouseHostBuilder.toHost(_, Some(8123))))
          })
      })
  }
}
