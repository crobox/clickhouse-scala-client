package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.DslTestSpec

class ModernColumnTypeTest extends DslTestSpec {

  it should "render DateTime64 with a precision" in {
    ColumnType.DateTime64(3).toString shouldBe "DateTime64(3)"
  }

  it should "render DateTime64 with a timezone" in {
    ColumnType.DateTime64(6, Option("Europe/Amsterdam")).toString shouldBe "DateTime64(6, 'Europe/Amsterdam')"
  }

  it should "accept the full precision range" in {
    ColumnType.DateTime64(0).toString shouldBe "DateTime64(0)"
    ColumnType.DateTime64(9).toString shouldBe "DateTime64(9)"
  }

  it should "refuse a precision ClickHouse does not have" in {
    an[IllegalArgumentException] should be thrownBy ColumnType.DateTime64(10)
    an[IllegalArgumentException] should be thrownBy ColumnType.DateTime64(-1)
  }

  it should "render Variant" in {
    ColumnType.Variant(ColumnType.UInt64, ColumnType.String).toString shouldBe "Variant(UInt64, String)"
  }

  it should "refuse a Variant with one alternative, which is just that type" in {
    an[IllegalArgumentException] should be thrownBy ColumnType.Variant(ColumnType.UInt64)
  }

  it should "render Dynamic and JSON" in {
    ColumnType.Dynamic.toString shouldBe "Dynamic"
    ColumnType.JSON.toString shouldBe "JSON"
  }

  it should "render JSON with its parameters in the order the server renders them back" in {
    ColumnType
      .JSON(
        maxDynamicPaths = Option(10),
        maxDynamicTypes = Option(3),
        typedPaths = Seq("a.b" -> ColumnType.UInt32, "c" -> ColumnType.String),
        skip = Seq("x.y"),
        skipRegexp = Seq("^tmp\\..*")
      )
      .toString shouldBe
    """JSON(max_dynamic_types=3, max_dynamic_paths=10, `a.b` UInt32, c String, SKIP `x.y`, SKIP REGEXP '^tmp\\..*')"""
  }

  it should "render JSON without parameters as plain JSON" in {
    ColumnType.JSON().toString shouldBe "JSON"
  }

  it should "refuse JSON limits the server does not accept" in {
    an[IllegalArgumentException] should be thrownBy ColumnType.JSON(maxDynamicTypes = Option(255))
    an[IllegalArgumentException] should be thrownBy ColumnType.JSON(maxDynamicPaths = Option(1000001))
  }

  it should "render SimpleAggregateFunction" in {
    ColumnType.SimpleAggregateFunction("sum", ColumnType.UInt64).toString shouldBe
    "SimpleAggregateFunction(sum, UInt64)"
  }

  it should "render SimpleAggregateFunction with several argument types" in {
    ColumnType.SimpleAggregateFunction("anyLast", ColumnType.UInt64, ColumnType.String).toString shouldBe
    "SimpleAggregateFunction(anyLast, UInt64, String)"
  }

  it should "nest inside the wrappers that take a type" in {
    ColumnType.Nullable(ColumnType.DateTime64(3)).toString shouldBe "Nullable(DateTime64(3))"
    ColumnType.Array(ColumnType.Dynamic).toString shouldBe "Array(Dynamic)"
    ColumnType.LowCardinality(ColumnType.JSON).toString shouldBe "LowCardinality(JSON)"
  }
}
