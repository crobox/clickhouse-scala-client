package com.crobox.clickhouse.stream

import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.crobox.clickhouse.ClickhouseClient
import com.crobox.clickhouse.stream.ClickhouseBulkActor.{InsertParsed, InsertRaw}
import com.crobox.clickhouse.test.ClickhouseClientSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.collection.mutable
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Random, Try}

class ClickhouseIndexingSubscriberTest
    extends ClickhouseClientSpec
    with BeforeAndAfterEach
    with ScalaFutures
    with Eventually {

  val timeout = 10.seconds

  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  val client: ClickhouseClient = new ClickhouseClient(config)

  var subscriberCompletes: Promise[Unit] = Promise[Unit]

  val createDb    = "CREATE DATABASE IF NOT EXISTS test"
  val dropDb   = "DROP DATABASE IF EXISTS test"
  val createTable = """CREATE TABLE test.insert
                      |(
                      |    i UInt64,
                      |    s String,
                      |    a Array(UInt32)
                      |) ENGINE = Memory""".stripMargin

  override protected def beforeEach(): Unit = {
    super.beforeAll()

    Await.ready(for {
      db     <- client.execute(createDb)
      create <- client.execute(createTable)
    } yield create, timeout)

    subscriberCompletes = Promise[Unit]
  }

  override protected def afterEach(): Unit = {
    super.afterEach()

    Await.ready(client.execute(dropDb), timeout)
  }

  implicit val patience = PatienceConfig(10.seconds, 1000.millis)

  val unparsedInserts: Seq[Map[String, Any]] = (1 to 10).map(
    _ =>
      Map(
        "i" -> Random.nextInt(100),
        "s" -> "two",
        "a" -> (1 to Random.nextInt(20)).map(_ => Random.nextInt(200))
    )
  )

  val parsedInserts: Seq[Map[String, String]] = unparsedInserts.map(_.mapValues({
    case value: Int             => value.toString
    case value: String          => "'" + value + "'"
    case value: IndexedSeq[Int] => "[" + value.mkString(", ") + "]"
  }))

  def testSubscriber = new ClickhouseIndexingSubscriber(client,
    SubscriberConfig(
      batchSize = 10,
      flushInterval = Some(10.millis),
      flushAfter = Some(10.millis),
      successCallback = (a,b) => {
        if (!subscriberCompletes.isCompleted) subscriberCompletes.complete(Try{}); Unit
      }))

  it should "inject sql parsed rows" in {
    val sink = Sink.fromSubscriber(testSubscriber)

    Source
      .fromIterator(() => parsedInserts.toIterator)
      .map(data => InsertParsed("test.insert", data))
      .toMat(sink)(Keep.left)
      .run()

    Await.ready(subscriberCompletes.future, 5.seconds)

    eventually {
      checkRowCount().futureValue shouldBe parsedInserts.size
    }
  }

  it should "inject unparsed rows" in {
    val sink = Sink.fromSubscriber(testSubscriber)

    Source
      .fromIterator(() => unparsedInserts.toIterator)
      .map(data => InsertRaw("test.insert", data))
      .toMat(sink)(Keep.left)
      .run()

    Await.ready(subscriberCompletes.future, 5.seconds)

    eventually {
      checkRowCount().futureValue shouldBe unparsedInserts.size
    }
  }

  private def checkRowCount(): Future[Int] = client.query("SELECT count(*) FROM test.insert").map(res =>
    Try(res.stripLineEnd.toInt).getOrElse(0)
  )
}
