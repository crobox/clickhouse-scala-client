package com.crobox.clickhouse.dsl.execution
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink}
import com.crobox.clickhouse.dsl.parallel._
import com.crobox.clickhouse.internal.ClickHouseExecutor.{QueryAccepted, QueryFinished}
import com.crobox.clickhouse.testkit.ClickhouseClientSpec
import com.crobox.clickhouse.{DslLanguage, TestSchemaClickhouseQuerySpec}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import spray.json.DefaultJsonProtocol._

class ClickhouseQueryExecutorIT
    extends ClickhouseClientSpec
    with TestSchemaClickhouseQuerySpec
    with ScalaFutures
    with DslLanguage {
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(10, Seconds)), interval = scaled(Span(20, Millis)))

  implicit val materializer = ActorMaterializer()
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
