package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl.JoinQuery.InnerJoin
import com.crobox.clickhouse.dsl.execution.{DefaultClickhouseQueryExecutor, QueryResult}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import java.util.UUID
import scala.concurrent.Future
import scala.util.Random

class WithQueryIT extends DslITSpec {

  private val oneId = UUID.randomUUID()
  override val table1Entries = 
    Seq(Table1Entry(oneId), Table1Entry(randomUUID), Table1Entry(randomUUID), Table1Entry(randomUUID))
  override val table2Entries = Seq(Table2Entry(oneId, randomString, Random.nextInt(1000) + 1, randomString, None))

  it should "execute WITH clause with constant value" in {
    case class Result(shield_id: UUID, constant_value: Int)
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat2(Result)

    val query = withExpression("constant_value", const(100))
      .select(shieldId, ref[Int]("constant_value"))
      .from(OneTestTable)
      .limit(Some(Limit(1)))

    val results: Future[QueryResult[Result]] = queryExecutor.execute[Result](query)
    val result = results.futureValue
    result.rows should not be empty
    result.rows.head.constant_value should be(100)
  }

  it should "execute WITH clause with random function" in {
    case class Result(shield_id: UUID, random_value: Double)
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat2(Result)

    val query = withExpression("random_value", rand())
      .select(shieldId, ref[Double]("random_value"))
      .from(OneTestTable)
      .limit(Some(Limit(1)))

    val results: Future[QueryResult[Result]] = queryExecutor.execute[Result](query)
    val result = results.futureValue
    result.rows should not be empty
    result.rows.head.random_value should be >= 0.0
    result.rows.head.random_value should be <= 1.0
  }

  it should "execute WITH clause with multiple expressions" in {
    case class Result(shield_id: UUID, total: Int)
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat2(Result)

    val query = withExpressions(
      "value1" -> const(25),
      "value2" -> const(75),
      "total" -> (ref[Int]("value1") + ref[Int]("value2"))
    ).select(shieldId, ref[Int]("total"))
      .from(OneTestTable)
      .limit(Some(Limit(1)))

    val results: Future[QueryResult[Result]] = queryExecutor.execute[Result](query)
    val result = results.futureValue
    result.rows should not be empty
    result.rows.head.total should be(100)
  }

  it should "execute WITH clause with column expression" in {
    case class Result(shield_id: UUID, shield_as_string: String)
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat2(Result)

    val query = withExpression("shield_as_string", toStringRep(shieldId))
      .select(shieldId, ref[String]("shield_as_string"))
      .from(OneTestTable)
      .limit(Some(Limit(1)))

    val results: Future[QueryResult[Result]] = queryExecutor.execute[Result](query)
    val result = results.futureValue
    result.rows should not be empty
    result.rows.head.shield_as_string should be(result.rows.head.shield_id.toString)
  }

  it should "execute WITH clause with subquery reference" in {
    case class Result(shield_id: UUID, max_column_2: Int)
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat2(Result)

    // First create a WITH expression that selects the max value from col2
    val query = withExpression("max_col2", max(col2))
      .select(shieldId, ref[Int]("max_col2"))
      .from(OneTestTable)
      .join(InnerJoin, TwoTestTable)
      .using(shieldId)
      .limit(Some(Limit(1)))

    val results: Future[QueryResult[Result]] = queryExecutor.execute[Result](query)
    val result = results.futureValue
    result.rows should not be empty
    result.rows.head.max_column_2 should be > 0
  }

  it should "execute WITH clause in combination with WHERE clause" in {
    case class Result(shield_id: UUID, threshold: Int)
    implicit val resultFormat: RootJsonFormat[Result] = jsonFormat2(Result)

    val testUuid = table1Entries.head.shieldId
    val query = withExpression("threshold", const(50))
      .select(shieldId, ref[Int]("threshold"))
      .from(OneTestTable)
      .where(shieldId.isEq(testUuid))
      .limit(Some(Limit(1)))

    val results: Future[QueryResult[Result]] = queryExecutor.execute[Result](query)
    val result = results.futureValue
    result.rows should not be empty
    result.rows.head.shield_id should be(testUuid)
    result.rows.head.threshold should be(50)
  }

  def runQry(query: OperationalQuery): Future[String] = {
    val che = queryExecutor.asInstanceOf[DefaultClickhouseQueryExecutor]
    clickClient.query(che.toSql(query.internalQuery))
  }
}