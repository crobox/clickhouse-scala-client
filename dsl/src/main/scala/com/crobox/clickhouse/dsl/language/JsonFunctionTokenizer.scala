package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType

trait JsonFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeJsonFunction(col: JsonFunction[_])(implicit ctx: TokenizeContext): String = {
    val command = col match {
      case VisitParamHas(_, _)           => "visitParamHas"
      case VisitParamExtractUInt(_, _)   => "visitParamExtractUInt"
      case VisitParamExtractInt(_, _)    => "visitParamExtractInt"
      case VisitParamExtractFloat(_, _)  => "visitParamExtractFloat"
      case VisitParamExtractBool(_, _)   => "visitParamExtractBool"
      case VisitParamExtractRaw(_, _)    => "visitParamExtractRaw"
      case VisitParamExtractString(_, _) => "visitParamExtractString"
    }

    s"$command(${tokenizeColumn(col.params.column)},${tokenizeColumn(col.fieldName.column)})"
  }

  def tokenizeJsonFunctionCol(col: JsonFunctionCol[_])(implicit ctx: TokenizeContext): String = {
    def call(function: String, args: Column*): String = s"$function(${tokenizeColumns(args)})"

    def path(json: Magnet[_], keys: Seq[Magnet[_]], typeName: Option[ColumnType] = None): Seq[Column] =
      (json +: keys).map(_.column) ++ typeName.map(t => Const(t.toString))

    def extract(function: String, caseInsensitive: Boolean, args: Seq[Column]): String =
      call(if (caseInsensitive) function + "CaseInsensitive" else function, args: _*)

    col match {
      case JSONHas(json, keys)    => call("JSONHas", path(json, keys): _*)
      case JSONLength(json, keys) => call("JSONLength", path(json, keys): _*)
      case JSONType(json, keys)   => call("JSONType", path(json, keys): _*)
      case JSONKey(json, keys)    => call("JSONKey", path(json, keys): _*)

      case JSONExtractUInt(json, keys, ci)             => extract("JSONExtractUInt", ci, path(json, keys))
      case JSONExtractInt(json, keys, ci)              => extract("JSONExtractInt", ci, path(json, keys))
      case JSONExtractFloat(json, keys, ci)            => extract("JSONExtractFloat", ci, path(json, keys))
      case JSONExtractBool(json, keys, ci)             => extract("JSONExtractBool", ci, path(json, keys))
      case JSONExtractString(json, keys, ci)           => extract("JSONExtractString", ci, path(json, keys))
      case JSONExtractRaw(json, keys, ci)              => extract("JSONExtractRaw", ci, path(json, keys))
      case JSONExtractArrayRaw(json, keys, ci)         => extract("JSONExtractArrayRaw", ci, path(json, keys))
      case JSONExtractKeys(json, keys, ci)             => extract("JSONExtractKeys", ci, path(json, keys))
      case JSONExtractKeysAndValuesRaw(json, keys, ci) => extract("JSONExtractKeysAndValuesRaw", ci, path(json, keys))
      case JSONExtractKeysAndValues(json, valueType, keys, ci) =>
        extract("JSONExtractKeysAndValues", ci, path(json, keys, Option(valueType)))
      case JSONExtract(json, returnType, keys, ci) => extract("JSONExtract", ci, path(json, keys, Option(returnType)))

      case JSONArrayLength(json)              => call("JSONArrayLength", json.column)
      case IsValidJSON(json)                  => call("isValidJSON", json.column)
      case JSONMergePatch(json, more)         => call("JSONMergePatch", (json +: more).map(_.column): _*)
      case JSONExists(json, jsonPath)         => call("JSON_EXISTS", json.column, jsonPath.column)
      case JSONQuery(json, jsonPath)          => call("JSON_QUERY", json.column, jsonPath.column)
      case JSONValue(json, jsonPath)          => call("JSON_VALUE", json.column, jsonPath.column)
      case ToJSONString(value)                => call("toJSONString", value.column)
      case JSONAllPaths(json)                 => call("JSONAllPaths", json.column)
      case JSONAllPathsWithTypes(json)        => call("JSONAllPathsWithTypes", json.column)
      case JSONDynamicPaths(json)             => call("JSONDynamicPaths", json.column)
      case JSONDynamicPathsWithTypes(json)    => call("JSONDynamicPathsWithTypes", json.column)
      case JSONSharedDataPaths(json)          => call("JSONSharedDataPaths", json.column)
      case JSONSharedDataPathsWithTypes(json) => call("JSONSharedDataPathsWithTypes", json.column)

      case DynamicType(dynamic)                  => call("dynamicType", dynamic.column)
      case DynamicElement(dynamic, elementType)  => call("dynamicElement", dynamic.column, Const(elementType.toString))
      case IsDynamicElementInSharedData(dynamic) => call("isDynamicElementInSharedData", dynamic.column)
      case GetSubcolumn(value, subcolumn)        => call("getSubcolumn", value.column, Const(subcolumn))

      case access: JSONPathAccess[_] => tokenizeJsonPathAccess(access)
    }
  }

  private def tokenizeJsonPathAccess(access: JSONPathAccess[_])(implicit ctx: TokenizeContext): String = {
    val base = tokenizeColumn(access.json)
    val keys = access.path.map(ClickhouseStatement.quoteIdentifier)
    access match {
      case JSONSubcolumn(_, _) => (base +: keys).mkString(".")
      // Quoted whole, so a parameterised type's parentheses and commas stay one token: `.:`Array(JSON)``.
      case JSONTypedSubcolumn(_, _, columnType) =>
        (base +: keys).mkString(".") + ".:" + ClickhouseStatement.quoteIdentifier(columnType.toString)
      case JSONSubObject(_, _)      => s"$base.^${keys.mkString(".")}"
      case JSONArrayOfObjects(_, _) => (base +: keys).mkString(".") + "[]"
    }
  }
}
