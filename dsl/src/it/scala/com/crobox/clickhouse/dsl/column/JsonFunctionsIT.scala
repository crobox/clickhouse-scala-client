package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import spray.json._

class JsonFunctionsIT extends DslITSpec {

  private val doc = """{"a":{"b":[1,2],"s":"x"},"n":-3,"f":1.5,"t":true,"arr":[{"x":1},{"x":2}]}"""

  private val j = ref[JsValue]("j")

  private def fromJson(columns: Column*): String =
    runSql(select(columns: _*).from(select(cast(doc, ColumnType.JSON) as "j"))).futureValue.trim

  it should "succeed for JsonFunctions" in {
    val someJson = """{"foo":"bar", "baz":123, "boz":3.1415, "bool":true}"""

    r(visitParamHas(someJson, "foo")) shouldBe "1"
    r(visitParamExtractUInt(someJson, "baz")) shouldBe "123"
    r(visitParamExtractInt(someJson, "baz")) shouldBe "123"
    r(visitParamExtractFloat(someJson, "boz")) shouldBe "3.1415"
    r(visitParamExtractBool(someJson, "bool")) shouldBe "1"
    r(visitParamExtractRaw(someJson, "foo")) shouldBe "\"bar\""
    r(visitParamExtractString(someJson, "foo")) shouldBe "bar"

    r(simpleJSONHas(someJson, "foo")) shouldBe "1"
    r(simpleJSONExtractString(someJson, "foo")) shouldBe "bar"
  }

  it should "walk a path of keys and indices" in {
    r(jsonHas(doc, "a", "b", 2)) shouldBe "1"
    r(jsonHas(doc, "a", "b", 3)) shouldBe "0"
    r(jsonLength(doc, "a", "b")) shouldBe "2"
    r(jsonType(doc, "a")) shouldBe "Object"
    r(jsonKey(doc, 2)) shouldBe "n"
    r(jsonExtractUInt(doc, "a", "b", -1)) shouldBe "2"
    r(jsonExtractInt(doc, "n")) shouldBe "-3"
    r(jsonExtractFloat(doc, "f")) shouldBe "1.5"
    r(jsonExtractBool(doc, "t")) shouldBe "1"
    r(jsonExtractString(doc, "a", "s")) shouldBe "x"
    r(jsonExtractRaw(doc, "a", "b")) shouldBe "[1,2]"
    r(jsonExtractArrayRaw(doc, "arr")) shouldBe """['{"x":1}','{"x":2}']"""
    r(jsonExtractKeys(doc, "a")) shouldBe "['b','s']"
    r(jsonExtractKeysAndValuesRaw(doc, "a")) shouldBe """[('b','[1,2]'),('s','"x"')]"""
  }

  it should "extract into a declared type" in {
    r(jsonExtract[Seq[Long]](doc, ColumnType.Array(ColumnType.Int64), "a", "b")) shouldBe "[1,2]"
    r(jsonExtract[Option[Long]](doc, ColumnType.Nullable(ColumnType.Int64), "missing")) shouldBe "\\N"
    r(jsonExtractKeysAndValues[Long]("""{"x":1,"y":2}""", ColumnType.Int64)) shouldBe "[('x',1),('y',2)]"
  }

  it should "ignore the case of keys in the CaseInsensitive variants" in {
    assume(serverAtLeast(25, 8), "the CaseInsensitive variants arrived in 25.8")
    r(jsonExtractStringCaseInsensitive(doc, "A", "S")) shouldBe "x"
    r(jsonExtractCaseInsensitive[Seq[Long]](doc, ColumnType.Array(ColumnType.Int64), "A", "B")) shouldBe "[1,2]"
  }

  it should "run the remaining String functions" in {
    r(jsonArrayLength("[1,2,3]")) shouldBe "3"
    r(jsonArrayLength("{}")) shouldBe "\\N"
    r(isValidJSON(doc)) shouldBe "1"
    r(isValidJSON("{")) shouldBe "0"
    r(jsonMergePatch("""{"a":1}""", """{"b":2}""", """{"a":3}""")) shouldBe """{"a":3,"b":2}"""
    r(jsonExists(doc, "$.a.s")) shouldBe "1"
    r(jsonQuery(doc, "$.a.b")) shouldBe "[[1,2]]"
    r(jsonValue(doc, "$.a.s")) shouldBe "x"
    r(toJSONString(tuple(1, "a"))) shouldBe """[1,"a"]"""
  }

  it should "introspect a JSON value" in {
    fromJson(jsonAllPaths(j)) shouldBe "['a.b','a.s','arr','f','n','t']"
    fromJson(jsonDynamicPaths(j)) shouldBe "['a.b','a.s','arr','f','n','t']"
    fromJson(jsonSharedDataPaths(j)) shouldBe "[]"
    fromJson(jsonAllPathsWithTypes(j)) should include("'a.s':'String'")
    fromJson(jsonDynamicPathsWithTypes(j)) should include("'n':'Int64'")
    fromJson(jsonSharedDataPathsWithTypes(j)) shouldBe "{}"
  }

  it should "read paths with dot syntax" in {
    fromJson(jsonSubcolumn(j, "a", "s")) shouldBe "x"
    fromJson(jsonTypedSubcolumn[Long](j, ColumnType.Int64, "n")) shouldBe "-3"
    fromJson(jsonTypedSubcolumn[Long](j, ColumnType.Int64, "a", "s")) shouldBe "\\N"
    fromJson(jsonSubcolumn(jsonArrayOfObjects(j, "arr"), "x")) shouldBe "[1,2]"
    fromJson(getSubcolumn(j, "a.s")) shouldBe "x"
    fromJson(toTypeName(jsonSubObject(j, "a"))) shouldBe "JSON"
    fromJson(toTypeName(jsonArrayOfObjects(j, "arr"))) should startWith("Array(JSON")
  }

  it should "inspect a Dynamic value" in {
    fromJson(dynamicType(jsonSubcolumn(j, "n"))) shouldBe "Int64"
    fromJson(dynamicElement[Long](jsonSubcolumn(j, "n"), ColumnType.Int64)) shouldBe "-3"
    fromJson(dynamicElement[Long](jsonSubcolumn(j, "a", "s"), ColumnType.Int64)) shouldBe "\\N"
    fromJson(isDynamicElementInSharedData(jsonSubcolumn(j, "n"))) shouldBe "false"
  }

  it should "aggregate over JSON values" in {
    fromJson(distinctJSONPaths(j)) shouldBe "['a.b','a.s','arr','f','n','t']"
    fromJson(distinctJSONPathsAndTypes(j)) should include("'n':['Int64']")
    fromJson(distinctDynamicTypes(jsonSubcolumn(j, "n"))) shouldBe "['Int64']"
  }

  it should "decode maps, pairs and JSON values through executeRows" in {
    val paths = jsonAllPathsWithTypes(j) as "paths"
    val pairs = jsonExtractKeysAndValuesRaw(doc, "a") as "pairs"
    val sub   = jsonSubObject(j, "a") as "sub"
    val row   = queryExecutor
      .executeRows(select(paths, pairs, sub).from(select(cast(doc, ColumnType.JSON) as "j")))
      .futureValue
      .rows
      .head

    row.get(paths).flatMap(_.get("a.s")) shouldBe Some("String")
    row.get(pairs).map(_.toSeq) shouldBe Some(Seq("b" -> "[1,2]", "s" -> "\"x\""))
    row.get(sub).map(_.asJsObject.fields("s")) shouldBe Some(JsString("x"))
  }

  private def serverAtLeast(major: Int, minor: Int): Boolean =
    clickClient.query("SELECT version()").futureValue.trim.split('.').take(2).map(_.toInt).toSeq match {
      case Seq(serverMajor, serverMinor) => Ordering[(Int, Int)].gteq((serverMajor, serverMinor), (major, minor))
      case other                         => fail(s"Unexpected server version ${other.mkString(".")}")
    }
}
