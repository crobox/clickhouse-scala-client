package com.crobox.clickhouse.stream

import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.crobox.clickhouse.stream.ClickhouseBulkActor.Insert
import com.crobox.clickhouse.{ClickhouseClient, ClickhouseClientAsyncSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Random, Try}

class ClickhouseIndexingSubscriberTest extends ClickhouseClientAsyncSpec with ScalaFutures with Eventually {

  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  val client: ClickhouseClient = new ClickhouseClient(config)

  var subscriberCompletes: Promise[Unit] = Promise[Unit]

  val createDb    = "CREATE DATABASE IF NOT EXISTS test"
  val dropDb      = "DROP DATABASE IF EXISTS test"
  val createTable = """CREATE TABLE test.insert
                      |(
                      |    i UInt64,
                      |    s String,
                      |    a Array(UInt32)
                      |) ENGINE = Memory""".stripMargin

  override protected def beforeEach(): Unit = {
    super.beforeAll()

    Await.ready(for {
      _ <- client.execute(createDb)
      create <- client.execute(createTable)
    } yield create, timeout.duration)

    subscriberCompletes = Promise[Unit]
  }

  override protected def afterEach(): Unit = {
    super.afterEach()

    Await.ready(client.execute(dropDb), timeout.duration)
  }

  def unparsedInserts(key: String): Seq[Map[String, Any]] = (1 to 10).map(
    _ =>
      Map(
        "i" -> Random.nextInt(100),
        "s" -> key,
        "a" -> (1 to Random.nextInt(20)).map(_ => Random.nextInt(200))
    )
  )

  def parsedInserts(key: String) = unparsedInserts(key).map(
    _.mapValues({
      case value: Int           => value.toString
      case value: String        => "\"" + value + "\""
      case value: IndexedSeq[_] => "[" + value.mkString(", ") + "]"
    }).map { case (k, v) => s""""$k" : $v""" }
      .mkString(", ")
  )

  def testSubscriber =
    new ClickhouseIndexingSubscriber(
      client,
      SubscriberConfig(
        batchSize = 10,
        flushInterval = Some(10.millis),
        flushAfter = Some(10.millis),
        successCallback = (a, b) => {
          if (!subscriberCompletes.isCompleted) subscriberCompletes.complete(Try {}); Unit
        }
      )
    )

  it should "inject sql parsed rows" in {
    val sink    = Sink.fromSubscriber(testSubscriber)
    val inserts = parsedInserts("one")
    Source
      .fromIterator(() => inserts.toIterator)
      .map(data => Insert("test.insert", "{" + data + "}"))
      .toMat(sink)(Keep.left)
      .run()

    //Note that this would time-out if the subscriber fails to write the inserts
    Await.ready(subscriberCompletes.future, 5.seconds)
    checkRowCount("one").map(_ shouldBe inserts.size)
  }

  it should "index items" in {
    val inserts = parsedInserts("two")
    val res = Source
      .fromIterator(() => inserts.toIterator)
      .map(data => Insert("test.insert", "{" + data + "}"))
      .runWith(ClickhouseSink.insertSink(config, client))
    Await.ready(res, 5.seconds)
    checkRowCount("two").map(_ shouldBe inserts.size)
  }

  private def checkRowCount(key: String): Future[Int] =
    client
      .query(s"SELECT count(*) FROM test.insert WHERE s = '$key'")
      .map(res => Try(res.stripLineEnd.toInt).getOrElse(0))
}
