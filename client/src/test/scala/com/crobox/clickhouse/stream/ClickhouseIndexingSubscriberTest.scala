package com.crobox.clickhouse.stream

import com.crobox.clickhouse.internal.QuerySettings
import com.crobox.clickhouse.{ClickhouseClient, ClickhouseClientAsyncSpec}
import com.typesafe.config.ConfigFactory
import org.apache.pekko.stream.scaladsl._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Random, Try}

class ClickhouseIndexingSubscriberTest extends ClickhouseClientAsyncSpec with ScalaFutures with Eventually {

  import system.dispatcher

  val client: ClickhouseClient = new ClickhouseClient(Some(config))

  var subscriberCompletes: Promise[Unit] = Promise[Unit]()

  val createDb = "CREATE DATABASE IF NOT EXISTS test"
  val dropDb   = "DROP DATABASE IF EXISTS test"

  val createTable =
    """CREATE TABLE test.insert
      |(
      |    i UInt64,
      |    s String,
      |    a Array(UInt32)
      |) ENGINE = Memory""".stripMargin

  override protected def beforeEach(): Unit = {
    super.beforeAll()

    Await.ready(
      for {
        _      <- client.execute(createDb)
        create <- client.execute(createTable)
      } yield create,
      timeout.duration
    )

    subscriberCompletes = Promise[Unit]()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()

    Await.ready(client.execute(dropDb), timeout.duration)
  }

  def unparsedInserts(key: String): Seq[Map[String, Any]] = (1 to 10).map(_ =>
    Map(
      "i" -> Random.nextInt(100),
      "s" -> key,
      "a" -> (1 to Random.nextInt(20)).map(_ => Random.nextInt(200))
    )
  )

  def parsedInserts(key: String): Seq[String] = unparsedInserts(key).map(
    _.view
      .mapValues {
        case value: Int           => value.toString
        case value: String        => "\"" + value + "\""
        case value: IndexedSeq[_] => "[" + value.mkString(", ") + "]"
      }
      .map { case (k, v) => s""""$k" : $v""" }
      .mkString(", ")
  )

  it should "index items" in {
    val inserts = parsedInserts("two")
    val res     = Source
      .fromIterator(() => inserts.iterator)
      .map(data => Insert("test.insert", "{" + data + "}"))
      .runWith(ClickhouseSink.toSink(config, client, Some("no-overrides")))
    Await.ready(res, 5.seconds)
    checkRowCount("two").map(_ shouldBe inserts.size)
  }

  it should "emit each element's context only once its rows are inserted" in {
    val flowConfig = ConfigFactory
      .parseString("crobox.clickhouse.indexer.flow { batch-size = 20, flush-interval = 100 millis }")
      .withFallback(config)
    val rows =
      (i: Int) => if (i == 3) Seq.empty else parsedInserts("flow").map(data => Insert("test.insert", "{" + data + "}"))
    val elements = (1 to 5).map(i => rows(i) -> i)
    Source(elements)
      .via(ClickhouseSink.toFlow[Int](flowConfig, client, Some("flow")))
      .mapAsync(1)(i => checkRowCount("flow").map(i -> _))
      .runWith(Sink.seq)
      .map { emitted =>
        emitted.map(_._1) shouldBe (1 to 5)
        emitted.forall { case (i, count) => count >= elements.take(i).map(_._1.size).sum } shouldBe true
        emitted.last._2 shouldBe 40
      }
  }

  it should "fail the stream and emit none of a batch's contexts when its insert fails" in
    recoverToSucceededIf[ClickhouseIndexingException] {
      Source
        .single(Seq(Insert("test.no_such_table", "{}")) -> 1)
        .via(ClickhouseSink.toFlow[Int](config, client))
        .runWith(Sink.seq)
    }

  it should "optimize items" in {
    var statements = Seq.empty[String]
    val settings   = QuerySettings()
    val client     = new ClickhouseClient(Some(config)) {
      override def execute(sql: String)(implicit settings: QuerySettings): Future[String] = {
        statements ++= Seq(sql)
        Future.successful("")
      }
    }
    ClickhouseSink.optimizeTable(client, Optimize(table = "distributed"))(dispatcher, settings)
    statements.last should be("OPTIMIZE TABLE distributed FINAL")

    ClickhouseSink
      .optimizeTable(client, Optimize(table = "distributed", localTable = Option("local")))(dispatcher, settings)
    statements.last should be("OPTIMIZE TABLE local FINAL")

    ClickhouseSink.optimizeTable(
      client,
      Optimize(table = "distributed", localTable = Option("local"), cluster = Option("cluster"))
    )(dispatcher, settings)
    statements.last should be("OPTIMIZE TABLE local ON CLUSTER cluster FINAL")

    ClickhouseSink.optimizeTable(
      client,
      Optimize(
        table = "distributed",
        localTable = Option("local"),
        cluster = Option("cluster"),
        partition = Option("ID abc")
      )
    )(dispatcher, settings)
    statements.last should be("OPTIMIZE TABLE local ON CLUSTER cluster PARTITION ID abc FINAL")
  }

  private def checkRowCount(key: String): Future[Int] =
    client
      .query(s"SELECT count(*) FROM test.insert WHERE s = '$key'")
      .map(res => Try(res.stripLineEnd.toInt).getOrElse(0))
}
