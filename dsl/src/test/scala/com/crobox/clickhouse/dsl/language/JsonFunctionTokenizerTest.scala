package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType

class JsonFunctionTokenizerTest extends DslTestSpec {

  private val j = NativeColumn[String]("j", ColumnType.JSON)

  private def sql(column: Column): String = toSQL(select(column), stripBeforeWhere = false).stripPrefix("SELECT").trim

  it should "render a path of keys and indices between the document and the type" in {
    sql(jsonExtractUInt("{}", "a", -1)) shouldBe "JSONExtractUInt('{}', 'a', -1)"
    sql(jsonExtract[Seq[Long]]("{}", ColumnType.Array(ColumnType.Int64), "a")) shouldBe
    "JSONExtract('{}', 'a', 'Array(Int64)')"
    sql(jsonExtractKeysAndValues[Long]("{}", ColumnType.Int64)) shouldBe "JSONExtractKeysAndValues('{}', 'Int64')"
  }

  it should "escape a type name that carries quotes" in {
    sql(jsonExtract[String]("{}", ColumnType.DateTime64(3, Option("UTC")))) shouldBe
    """JSONExtract('{}', 'DateTime64(3, \'UTC\')')"""
  }

  it should "add the suffix for the case-insensitive variants" in {
    sql(jsonExtractStringCaseInsensitive("{}", "A")) shouldBe "JSONExtractStringCaseInsensitive('{}', 'A')"
    sql(jsonExtractCaseInsensitive[Long]("{}", ColumnType.Int64, "A")) shouldBe
    "JSONExtractCaseInsensitive('{}', 'A', 'Int64')"
  }

  it should "render the SQL/JSON functions under their underscored names" in {
    sql(jsonExists("{}", "$.a")) shouldBe "JSON_EXISTS('{}', '$.a')"
    sql(jsonQuery("{}", "$.a")) shouldBe "JSON_QUERY('{}', '$.a')"
    sql(jsonValue("{}", "$.a")) shouldBe "JSON_VALUE('{}', '$.a')"
  }

  it should "render dot syntax" in {
    sql(jsonSubcolumn(j, "a", "b")) shouldBe "j.a.b"
    sql(jsonTypedSubcolumn[Long](j, ColumnType.Int64, "a")) shouldBe "j.a.:Int64"
    sql(jsonTypedSubcolumn[Seq[Long]](j, ColumnType.Array(ColumnType.Int64), "a")) shouldBe "j.a.:`Array(Int64)`"
    sql(jsonSubObject(j, "a", "b")) shouldBe "j.^a.b"
    sql(jsonArrayOfObjects(j, "arr")) shouldBe "j.arr[]"
    sql(jsonSubcolumn(jsonArrayOfObjects(j, "arr"), "x")) shouldBe "j.arr[].x"
    sql(jsonArrayOfObjects(jsonArrayOfObjects(j, "arr"))) shouldBe "j.arr[][]"
  }

  it should "quote a key that is not a plain identifier" in {
    sql(jsonSubcolumn(j, "a-b", "c d")) shouldBe "j.`a-b`.`c d`"
  }

  it should "refuse dot syntax without a path" in {
    an[IllegalArgumentException] should be thrownBy jsonSubcolumn(j)
    an[IllegalArgumentException] should be thrownBy jsonArrayOfObjects(j)
  }

  it should "render the JSON aggregates" in {
    sql(distinctJSONPaths(j)) shouldBe "distinctJSONPaths(j)"
    sql(distinctJSONPathsAndTypes(j)) shouldBe "distinctJSONPathsAndTypes(j)"
    sql(distinctDynamicTypes(jsonSubcolumn(j, "a"))) shouldBe "distinctDynamicTypes(j.a)"
  }

  it should "cast to JSON" in {
    sql(cast("{}", ColumnType.JSON)) shouldBe "cast('{}' AS JSON)"
    sql(cast("{}", ColumnType.JSON(maxDynamicPaths = Option(8)))) shouldBe "cast('{}' AS JSON(max_dynamic_paths=8))"
  }
}
