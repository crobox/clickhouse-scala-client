package com.crobox.clickhouse

import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.pattern.ask
import org.apache.pekko.testkit.TestKit
import org.apache.pekko.util.Timeout
import org.apache.pekko.util.Timeout.durationToTimeout
import com.crobox.clickhouse.balancing.HostBalancer
import com.crobox.clickhouse.balancing.discovery.ConnectionManagerActor.GetConnection
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.GZIPOutputStream
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

abstract class ClickhouseClientAsyncSpec(val config: Config = ConfigFactory.load())
    extends TestKit(ActorSystem("clickhouseClientAsyncTestSystem", config.getConfig("crobox.clickhouse.client")))
    with AsyncFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  implicit val timeout: Timeout = 5.second

  override protected def afterAll(): Unit =
    try super.afterAll()
    finally Await.result(system.terminate(), 10.seconds)

  def requestParallelHosts(balancer: HostBalancer, connections: Int = 10): Future[Seq[Uri]] =
    Future.sequence(
      (1 to connections)
        .map(_ => {
          balancer.nextHost
        })
    )

  def getConnections(manager: ActorRef, connections: Int = 10): Future[Seq[Uri]] =
    Future.sequence(
      (1 to connections)
        .map(_ => {
          (manager ? GetConnection()).mapTo[Uri]
        })
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

  def compressGzip(content: String): Array[Byte] = {
    val arrOutputStream = new ByteArrayOutputStream()
    val zipOutputStream = new GZIPOutputStream(arrOutputStream)
    zipOutputStream.write(content.getBytes(UTF_8))
    zipOutputStream.close()
    arrOutputStream.toByteArray
  }
}
