package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.TestSchemaClickhouseQuerySpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.execution.ClickhouseQueryExecutor
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database
import com.crobox.clickhouse.testkit.ClickhouseClientSpec
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Future

class ColumnFunctionTest extends ClickhouseClientSpec with TestSchemaClickhouseQuerySpec with ScalaFutures with ClickhouseTokenizerModule {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val clickhouseClient = clickClient
  implicit val db: Database = clickhouseClient.database

  val ex = ClickhouseQueryExecutor.default(clickhouseClient)

  "ArithmeticFunctions" should "correctly execute" in {
    def leftRightFunctions[L,R,O](implicit ev: AritRetType[L,R,O]): Seq[(String,(_root_.com.crobox.clickhouse.dsl.NumericCol[L], _root_.com.crobox.clickhouse.dsl.NumericCol[R]) => _root_.com.crobox.clickhouse.dsl.ArithmeticFunctionOp[O] with Product with Serializable)] = Seq(
      ("5", plus[L, R, O]),
      ("1", minus[L, R, O]),
      ("6", multiply[L, R, O]),
      ("1.5", divide[L, R, O]),
      ("1", intDiv[L, R, O]),
      ("1", intDivOrZero[L, R, O]),
      ("1", modulo[L, R, O]),
      ("1", gcd[L, R, O]),
      ("6", lcm[L, R, O])
    )

    leftRightFunctions[Int,Int,Int].foreach(fun =>{
      val qry = select(fun._2.apply(3,2))
      runSql(qry).futureValue.trim shouldBe fun._1
    })

    runSql(select(negate(3))).futureValue.trim shouldBe "-3"
    runSql(select(abs(-3))).futureValue.trim shouldBe "3"
  }


  private def runSql(query: OperationalQuery): Future[String] = {
    clickhouseClient.query(toSql(query.internalQuery,None))
  }
}
