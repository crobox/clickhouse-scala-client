package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.TestSchemaClickhouseQuerySpec
import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.column._
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

  "ColumnFunction tokenization and execution" should "succeed for ArithmeticFunctions" in {
    def leftRightFunctions[L,R,O](implicit ev: AritRetType[L,R,O]): Seq[(String,(NumericCol[L], NumericCol[R]) => ArithmeticFunctionOp[O])] = Seq(
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
      r(fun._2.apply(3,2)) shouldBe fun._1
    })

    r(negate(3)) shouldBe "-3"
    r(abs(-3)) shouldBe "3"
  }

  it should "succeed for ArrayFunctions" in {
    Seq(
      r(emptyArrayUInt8),
      r(emptyArrayUInt16),
      r(emptyArrayUInt32),
      r(emptyArrayUInt64),
      r(emptyArrayInt8),
      r(emptyArrayInt16),
      r(emptyArrayInt32),
      r(emptyArrayInt64),
      r(emptyArrayFloat32),
      r(emptyArrayFloat64),
      r(emptyArrayDate),
      r(emptyArrayDateTime),
      r(emptyArrayString)
    ).foreach(result => result shouldBe "[]")

    r(emptyArrayToSingle(emptyArrayUInt8)) shouldBe "[0]"
    r(range(3)) shouldBe "[0,1,2]"

    r(arrayConcat(range(2),Seq(2L,3L))) shouldBe "[0,1,2,3]"

    val arbArray = range(3)

    r(indexOf(arbArray,2L)) shouldBe "3"
    r(countEqual(arbArray,2L)) shouldBe "1"
    r(arrayEnumerate(arbArray)) shouldBe "[1,2,3]"
    r(arrayEnumerateUniq(arbArray)) shouldBe "[1,1,1]"
    r(arrayPopBack(arbArray)) shouldBe "[0,1]"
    r(arrayPopFront(arbArray)) shouldBe "[1,2]"
    r(arrayPushBack(arbArray,4L)) shouldBe "[0,1,2,4]"
    r(arrayPushFront(arbArray,4L)) shouldBe "[4,0,1,2]"
    r(arraySlice(arbArray,2,1)) shouldBe "[1]"
    r(arrayUniq(arbArray)) shouldBe "3"
    r(arrayJoin(arbArray)) shouldBe "0\n1\n2"
  }

  it should "succeed for BitFunctions" in {
    r(bitAnd(0,1)) shouldBe "0"
    r(bitOr(0,1)) shouldBe "1"
    r(bitXor(0,1)) shouldBe "1"
    r(bitNot(64)) shouldBe (255 - 64).toString
    r(bitShiftLeft(2,2)) shouldBe "8"
    r(bitShiftRight(8,1)) shouldBe "4"

  }

  it should "succeed for ComparisonFunctions" in {
    val someNum = const(10L)

    r(someNum <> 3) shouldBe "1"
    r(someNum > 3) shouldBe "1"
    r(someNum < 3) shouldBe "0"
    r(someNum >= 3) shouldBe "1"
    r(someNum <= 3) shouldBe "0"
    r(someNum isEq 3) shouldBe "0"
    r(notEquals(1,2)) shouldBe "1"
    r(_equals(2L,2)) shouldBe "1"
    r(less(1.0,200)) shouldBe "1"
    r(greater(1L,2L)) shouldBe "0"
    r(lessOrEquals(1,2)) shouldBe "1"
    r(greaterOrEquals(1,2)) shouldBe "0"
  }

  it should "succeed for DateTimeFunctions" in {}

  it should "succeed for DictionaryFunctions" in {}
  it should "succeed for EncodingFunctions" in {}
  it should "succeed for HashFunctions" in {}
  it should "succeed for HigherOrderFunctions" in {}
  it should "succeed for IPFunctions" in {}
  it should "succeed for JsonFunctions" in {}
  it should "succeed for LogicalFunctions" in {}
  it should "succeed for MathFunctions" in {}
  it should "succeed for MiscFunctions" in {}
  it should "succeed for RandomFunctions" in {}
  it should "succeed for RoundingFunctions" in {}
  it should "succeed for SplitMergeFunctions" in {}
  it should "succeed for StringFunctions" in {}
  it should "succeed for StringSearchFunctions" in {}
  it should "succeed for TypeCastFunctions" in {}
  it should "succeed for URLFunctions" in {}

  private def r(query: AnyTableColumn): String = {
    runSql(select(query)).futureValue.trim
  }

  private def runSql(query: OperationalQuery): Future[String] = {
    clickhouseClient.query(toSql(query.internalQuery,None))
  }
}
