package com.crobox.clickhouse.dsl.execution
import akka.stream.scaladsl.{Keep, Sink}
import com.crobox.clickhouse.TestSchemaClickhouseQuerySpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.parallel._
import com.crobox.clickhouse.internal.progress.QueryProgress.{QueryAccepted, QueryFinished}
import com.crobox.clickhouse.ClickhouseClientSpec
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol._

class ClickhouseQueryExecutorIT
    extends ClickhouseClientSpec
    with TestSchemaClickhouseQuerySpec
    with ScalaFutures {

  override implicit def patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))


  case class Result(one: Int)
  object Result {
    implicit val format = jsonFormat1(Result.apply)
  }
  "Combined queries" should "return merged query result" in {
    val result = chExecuter
      .executeWithProgress[Result](
        select(plus(const(1), const(1)).aliased("one")).combine(select(plus(const(1), const(2)).aliased("one")))
      )
    val queryResult = result.toMat(Sink.seq)(Keep.left).run()
    queryResult.futureValue shouldBe QueryResult(Seq(Result(2), Result(3)))
  }

  it should "return merged progress" in {
    val result = chExecuter
      .executeWithProgress[Result](
        select(plus(const(1), const(1)).aliased("one")).combine(select(plus(const(1), const(2)).aliased("one")))
      )
    val progressResult = result.toMat(Sink.seq)(Keep.right).run()
    progressResult.futureValue should contain theSameElementsAs Seq(QueryAccepted,
                                                                    QueryAccepted,
                                                                    QueryFinished,
                                                                    QueryFinished)
  }
}
