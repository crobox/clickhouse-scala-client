package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.testkit.ClickhouseMatchers
import com.crobox.clickhouse.{ClickhouseClientSpec, ClickhouseSQLSupport}

class AggregationFunctionTokenizerTest
    extends ClickhouseClientSpec
    with TestSchema
    with ClickhouseTokenizerModule
    with ClickhouseSQLSupport
    with ClickhouseMatchers {
  val database = "default"

  it should "arrayElement in groupArray" in {
    toSQL(select(arrayElement(groupArray(col1), 1) as "p"), false) should matchSQL("SELECT groupArray(column_1)[1] AS p")
  }
}
