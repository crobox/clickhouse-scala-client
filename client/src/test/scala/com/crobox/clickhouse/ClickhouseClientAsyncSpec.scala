package com.crobox.clickhouse

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model.Uri
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.GetConnection
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.Status.Alive
import com.crobox.clickhouse.balancing.discovery.health.HostHealthChecker.{HostStatus, IsAlive, Status}
import com.crobox.clickhouse.balancing.iterator.CircularIteratorSet
import com.crobox.clickhouse.internal.ClickhouseHostBuilder
import com.typesafe.config.ConfigFactory
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class ClickhouseClientAsyncSpec
    extends TestKit(ActorSystem("clickhouseClientTestSystem"))
    with AsyncFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  implicit val timeout      = durationToTimeout(5 second)
  implicit val materializer = ActorMaterializer()
  var probe: TestProbe      = _

  var uris: Map[Uri, Uri => Props] = Map.empty

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    probe = TestProbe()
    uris = Map(
      (ClickhouseHostBuilder
         .toHost("localhost", Some(8245)),
       HostAliveMock.props(Seq(Alive))),
      (ClickhouseHostBuilder
         .toHost("127.0.0.1", None),
       HostAliveMock.props(Seq(Alive)))
    )
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }
  val config = ConfigFactory.load()

  class HostAliveMock(host: Uri, status: Seq[Status], testProbe: TestProbe) extends Actor with ActorLogging {
    val statuses = new CircularIteratorSet[Status](status)

    override def receive: Receive = {
      case msg @ IsAlive() =>
        sender ! HostStatus(host, statuses.next())
        testProbe.ref ! msg
    }
  }

  object HostAliveMock {

    def props(status: Seq[Status])(host: Uri) =
      Props(new HostAliveMock(host, status, probe))
  }

  def requestParallelHosts(balancer: HostBalancer, connections: Int = 10): Future[Seq[Uri]] =
    Future.sequence(
      (1 to connections).toParArray
        .map(_ => {
          balancer.nextHost
        })
        .seq
    )

  def getConnections(manager: ActorRef, connections: Int = 10): Future[Seq[Uri]] =
    Future.sequence(
      (1 to connections).toParArray
        .map(_ => {
          (manager ? GetConnection()).mapTo[Uri]
        })
        .seq
    )

  //  TODO change this methods to custom matchers
  def returnsConnectionsInRoundRobinFashion(manager: ActorRef, expectedConnections: Set[Uri]) = {
    val RequestConnectionsPerHost = 50
    getConnections(manager, RequestConnectionsPerHost * expectedConnections.size)
      .map(connections => {
        eachHostReceivedExpectedConnections(connections, expectedConnections, RequestConnectionsPerHost)
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

  def elementsDuplicated(values: Seq[Uri], elements: Int) =
    Iterator
      .continually(values)
      .flatten
      .take(elements)
      .toIndexedSeq
}
