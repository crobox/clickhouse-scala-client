package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.ClickhouseClientSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.ClickhouseTokenizerModule

class ArrayFunctionsTest extends ClickhouseClientSpec with TestSchema {
  val clickhouseTokenizer: ClickhouseTokenizerModule = new ClickhouseTokenizerModule {}
  override val database: String                      = "array_functions_test"

  val arrayNumbers           = Array(1, 2, 3, 4)
  val arrayNumbersSerialized = "[1, 2, 3, 4]"

  it should "arrayFunction: has" in {
    var query = select(All()).from(OneTestTable).where(has(numbers, 1))
    extractCondition(query) should be("has(numbers,1)")

    query = select(All()).from(OneTestTable).where(has(arrayNumbers, 1))
    extractCondition(query) should be(s"has($arrayNumbersSerialized,1)")
  }

  it should "arrayFunction: hasAll" in {
    var query = select(All()).from(OneTestTable).where(hasAll(numbers, Array(1, 2)))
    extractCondition(query) should be("hasAll(numbers,[1, 2])")

    query = select(All()).from(OneTestTable).where(hasAll(arrayNumbers, Array(1, 2)))
    extractCondition(query) should be(s"hasAll($arrayNumbersSerialized,[1, 2])")
  }

  it should "arrayFunction: hasAny" in {
    var query = select(All()).from(OneTestTable).where(hasAny(numbers, Array(1, 2)))
    extractCondition(query) should be("hasAny(numbers,[1, 2])")

    query = select(All()).from(OneTestTable).where(hasAny(arrayNumbers, Array(1, 2)))
    extractCondition(query) should be(s"hasAny($arrayNumbersSerialized,[1, 2])")
  }

  it should "arrayFunction: resize" in {
    var query = select(arrayResize(numbers, 4, 0)).from(OneTestTable)
    extractSelect(query) should be("arrayResize(numbers,4,0)")

    query = select(arrayResize(arrayNumbers, 4, 0)).from(OneTestTable)
    extractSelect(query) should be(s"arrayResize($arrayNumbersSerialized,4,0)")
  }

  def extractCondition(query: Query): String = {
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql.substring(sql.indexOf("WHERE") + 6, sql.indexOf(" FORMAT"))
  }

  def extractSelect(query: Query): String = {
    val sql = clickhouseTokenizer.toSql(query.internalQuery)
    sql.substring(sql.indexOf("SELECT") + 7, sql.indexOf(" FROM"))
  }
}
