package com.crobox.clickhouse.test

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.Uri
import akka.pattern.ask
import akka.testkit.TestKit
import akka.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.GetConnection
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.Alive
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{
  HostStatus,
  IsAlive,
  Status
}
import com.crobox.clickhouse.balancing.iterator.CircularIteratorSet
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.typesafe.config.ConfigFactory
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class ClickhouseClientAsyncSpec
    extends TestKit(ActorSystem("clickhouseClientTestSystem"))
    with AsyncFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {
  implicit val timeout = durationToTimeout(5 second)

  override protected def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }

  val config = ConfigFactory.load()
  val uris: Map[Uri, Uri => Props] = Map(
    (ClickhouseHostBuilder
       .toHost("localhost", 8245),
     HostAliveMock.props(Seq(Alive))),
    (ClickhouseHostBuilder
       .toHost("127.0.0.1"),
     HostAliveMock.props(Seq(Alive)))
  )

  class HostAliveMock(host: Uri, status: Seq[Status])
      extends Actor
      with ActorLogging {
    val statuses = new CircularIteratorSet[Status](status)

    override def receive: Receive = {
      case IsAlive() =>
        sender ! HostStatus(host, statuses.next())
    }
  }
  object HostAliveMock {
    def props(status: Seq[Status])(host: Uri) =
      Props(new HostAliveMock(host, status))
  }

  def requestParallelHosts(balancer: HostBalancer,
                           connections: Int = 10): Future[Seq[Uri]] = {
    Future.sequence(
      (1 to connections).toParArray
        .map(_ => {
          balancer.nextHost
        })
        .seq)
  }

  def getConnections(manager: ActorRef,
                     connections: Int = 10): Future[Seq[Uri]] = {
    Future.sequence(
      (1 to connections).toParArray
        .map(_ => {
          (manager ? GetConnection()).mapTo[Uri]
        })
        .seq)
  }
//  TODO change this methods to custom matchers
  def returnsConnectionsInRoundRobinFashion(manager: ActorRef,
                                            expectedConnections: Set[Uri]) = {
    import system.dispatcher
    val RequestConnectionsPerHost = 1000
    getConnections(manager,
                   RequestConnectionsPerHost * expectedConnections.size)
      .map(connections => {
        eachHostReceivedExpectedConnections(connections,
                                            expectedConnections,
                                            RequestConnectionsPerHost)
      })
  }

  def eachHostReceivedExpectedConnections(connections: Seq[Uri],
                                          expectedConnections: Set[Uri],
                                          RequestConnectionsPerHost: Int) = {
    val connectionsPerHost = expectedConnections.toSeq
      .map(uri => connections.count(_ == uri))
    val expectedConnectionsPerHost =
      expectedConnections.toSeq.map(_ => RequestConnectionsPerHost)
    connectionsPerHost should contain theSameElementsAs expectedConnectionsPerHost
  }

  def elementsDuplicated(values: Seq[Uri], elements: Int) = {
    Iterator
      .continually(values)
      .flatten
      .take(elements)
      .toIndexedSeq
  }
}
