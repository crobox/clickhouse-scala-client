package com.crobox.clickhouse.stream

import org.apache.pekko.stream.scaladsl._
import com.crobox.clickhouse.internal.QuerySettings
import com.crobox.clickhouse.{ClickhouseClient, ClickhouseClientAsyncSpec}
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

    Await.ready(for {
      _      <- client.execute(createDb)
      create <- client.execute(createTable)
    } yield create, timeout.duration)

    subscriberCompletes = Promise[Unit]()
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
    _.mapValues({ // do NOT change to .view.mapValues given compilation errors for scala 2.12.+
      case value: Int           => value.toString
      case value: String        => "\"" + value + "\""
      case value: IndexedSeq[_] => "[" + value.mkString(", ") + "]"
    }).map { case (k, v) => s""""$k" : $v""" }
      .mkString(", ")
  )

  it should "index items" in {
    val inserts = parsedInserts("two")
    val res = Source
      .fromIterator(() => inserts.toIterator) // do NOT change to .iterator given compilation errors for scala 2.12.+
      .map(data => Insert("test.insert", "{" + data + "}"))
      .runWith(ClickhouseSink.toSink(config, client, Some("no-overrides")))
    Await.ready(res, 5.seconds)
    checkRowCount("two").map(_ shouldBe inserts.size)
  }

  it should "optimize items" in {
    var statements = Seq.empty[String]
    val settings   = QuerySettings()
    val client = new ClickhouseClient(Some(config)) {
      override def execute(sql: String)(implicit settings: QuerySettings): Future[String] = {
        statements ++= Seq(sql)
        Future.successful("")
      }
    }
    ClickhouseSink.optimizeTable(client, Optimize(table = "distributed"))(dispatcher, settings)
    statements.last should be("OPTIMIZE TABLE distributed FINAL")

    ClickhouseSink.optimizeTable(client, Optimize(table = "distributed", localTable = Option("local")))(dispatcher,
                                                                                                        settings)
    statements.last should be("OPTIMIZE TABLE local FINAL")

    ClickhouseSink.optimizeTable(
      client,
      Optimize(table = "distributed", localTable = Option("local"), cluster = Option("cluster"))
    )(dispatcher, settings)
    statements.last should be("OPTIMIZE TABLE local ON CLUSTER cluster FINAL")

    ClickhouseSink.optimizeTable(client,
                                 Optimize(table = "distributed",
                                          localTable = Option("local"),
                                          cluster = Option("cluster"),
                                          partition = Option("ID abc")))(dispatcher, settings)
    statements.last should be("OPTIMIZE TABLE local ON CLUSTER cluster PARTITION ID abc FINAL")
  }

  private def checkRowCount(key: String): Future[Int] =
    client
      .query(s"SELECT count(*) FROM test.insert WHERE s = '$key'")
      .map(res => Try(res.stripLineEnd.toInt).getOrElse(0))
}
