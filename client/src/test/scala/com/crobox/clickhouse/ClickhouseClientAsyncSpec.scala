package com.crobox.clickhouse

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.Uri
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.GetConnection
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._

import scala.concurrent.Future
import scala.concurrent.duration._

class ClickhouseClientAsyncSpec
    extends TestKit(ActorSystem("clickhouseClientTestSystem"))
    with AsyncFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  val config: Config        = ConfigFactory.load()
  implicit val timeout      = durationToTimeout(5 second)
  implicit val materializer = ActorMaterializer()

  override protected def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
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
  def returnsConnectionsInRoundRobinFashion(manager: ActorRef, expectedConnections: Set[Uri]): Future[Assertion] = {
    val RequestConnectionsPerHost = 100
    getConnections(manager, RequestConnectionsPerHost * expectedConnections.size)
      .map(connections => {
        expectedConnections.foreach(
          uri =>
            connections
              .count(_ == uri) shouldBe (RequestConnectionsPerHost +- RequestConnectionsPerHost / 10) //10% delta for warm-up phase
        )
        succeed
      })
  }

}
