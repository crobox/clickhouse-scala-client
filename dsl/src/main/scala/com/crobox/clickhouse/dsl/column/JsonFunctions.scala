package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import com.crobox.clickhouse.dsl.{Column, ExpressionColumn, TableColumn}
import spray.json.JsValue

trait JsonFunctions { self: Magnets =>
  abstract class JsonFunction[T](val params: StringColMagnet[_], val fieldName: StringColMagnet[_])
      extends ExpressionColumn[T](params.column)

  case class VisitParamHas(_params: StringColMagnet[_], _fieldName: StringColMagnet[_])
      extends JsonFunction[Boolean](_params, _fieldName)
  case class VisitParamExtractUInt(_params: StringColMagnet[_], _fieldName: StringColMagnet[_])
      extends JsonFunction[Long](_params, _fieldName)
  case class VisitParamExtractInt(_params: StringColMagnet[_], _fieldName: StringColMagnet[_])
      extends JsonFunction[Long](_params, _fieldName)
  case class VisitParamExtractFloat(_params: StringColMagnet[_], _fieldName: StringColMagnet[_])
      extends JsonFunction[Float](_params, _fieldName)
  case class VisitParamExtractBool(_params: StringColMagnet[_], _fieldName: StringColMagnet[_])
      extends JsonFunction[Boolean](_params, _fieldName)
  case class VisitParamExtractRaw[T](_params: StringColMagnet[_], _fieldName: StringColMagnet[_])
      extends JsonFunction[T](_params, _fieldName)
  case class VisitParamExtractString(_params: StringColMagnet[_], _fieldName: StringColMagnet[_])
      extends JsonFunction[String](_params, _fieldName)

  def visitParamHas(params: StringColMagnet[_], fieldName: StringColMagnet[_]) = VisitParamHas(params, fieldName)
  def visitParamExtractUInt(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractUInt(params, fieldName)
  def visitParamExtractInt(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractInt(params, fieldName)
  def visitParamExtractFloat(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractFloat(params, fieldName)
  def visitParamExtractBool(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractBool(params, fieldName)
  def visitParamExtractRaw(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractRaw(params, fieldName)
  def visitParamExtractString(params: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractString(params, fieldName)

  // The server's own names for the visitParam* functions, which it keeps as aliases.
  def simpleJSONHas(json: StringColMagnet[_], fieldName: StringColMagnet[_])         = VisitParamHas(json, fieldName)
  def simpleJSONExtractUInt(json: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractUInt(json, fieldName)
  def simpleJSONExtractInt(json: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractInt(json, fieldName)
  def simpleJSONExtractFloat(json: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractFloat(json, fieldName)
  def simpleJSONExtractBool(json: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractBool(json, fieldName)
  def simpleJSONExtractRaw(json: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractRaw[String](json, fieldName)
  def simpleJSONExtractString(json: StringColMagnet[_], fieldName: StringColMagnet[_]) =
    VisitParamExtractString(json, fieldName)

  abstract class JsonFunctionCol[V](target: Column) extends ExpressionColumn[V](target)

  // JSON held in a String. `path` is the server's `indices_or_keys`: a String is an object key, an Int a 1-based
  // position that counts from the end when negative.

  case class JSONHas(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]])
      extends JsonFunctionCol[Boolean](json.column)
  case class JSONLength(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]])
      extends JsonFunctionCol[Long](json.column)
  case class JSONType(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]])
      extends JsonFunctionCol[String](json.column)
  case class JSONKey(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]])
      extends JsonFunctionCol[String](json.column)

  case class JSONExtractUInt(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]], caseInsensitive: Boolean)
      extends JsonFunctionCol[Long](json.column)
  case class JSONExtractInt(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]], caseInsensitive: Boolean)
      extends JsonFunctionCol[Long](json.column)
  case class JSONExtractFloat(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]], caseInsensitive: Boolean)
      extends JsonFunctionCol[Double](json.column)
  case class JSONExtractBool(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]], caseInsensitive: Boolean)
      extends JsonFunctionCol[Boolean](json.column)
  case class JSONExtractString(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]], caseInsensitive: Boolean)
      extends JsonFunctionCol[String](json.column)
  case class JSONExtractRaw(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]], caseInsensitive: Boolean)
      extends JsonFunctionCol[String](json.column)
  case class JSONExtractArrayRaw(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]], caseInsensitive: Boolean)
      extends JsonFunctionCol[Iterable[String]](json.column)
  case class JSONExtractKeys(json: StringColMagnet[_], path: Seq[ConstOrColMagnet[_]], caseInsensitive: Boolean)
      extends JsonFunctionCol[Iterable[String]](json.column)
  case class JSONExtractKeysAndValuesRaw(
      json: StringColMagnet[_],
      path: Seq[ConstOrColMagnet[_]],
      caseInsensitive: Boolean
  ) extends JsonFunctionCol[Iterable[(String, String)]](json.column)
  case class JSONExtractKeysAndValues[V](
      json: StringColMagnet[_],
      valueType: ColumnType,
      path: Seq[ConstOrColMagnet[_]],
      caseInsensitive: Boolean
  ) extends JsonFunctionCol[Iterable[(String, V)]](json.column)
  case class JSONExtract[V](
      json: StringColMagnet[_],
      returnType: ColumnType,
      path: Seq[ConstOrColMagnet[_]],
      caseInsensitive: Boolean
  ) extends JsonFunctionCol[V](json.column)

  case class JSONArrayLength(json: StringColMagnet[_]) extends JsonFunctionCol[Option[Long]](json.column)
  case class IsValidJSON(json: StringColMagnet[_])     extends JsonFunctionCol[Boolean](json.column)
  case class JSONMergePatch(json: StringColMagnet[_], more: Seq[StringColMagnet[_]])
      extends JsonFunctionCol[String](json.column)
  case class JSONExists(json: StringColMagnet[_], jsonPath: StringColMagnet[_])
      extends JsonFunctionCol[Boolean](json.column)
  case class JSONQuery(json: StringColMagnet[_], jsonPath: StringColMagnet[_])
      extends JsonFunctionCol[String](json.column)
  case class JSONValue(json: StringColMagnet[_], jsonPath: StringColMagnet[_])
      extends JsonFunctionCol[String](json.column)
  case class ToJSONString(col: ConstOrColMagnet[_]) extends JsonFunctionCol[String](col.column)

  // The native JSON type, and the Dynamic values its paths hold.

  case class JSONAllPaths(json: ConstOrColMagnet[_])          extends JsonFunctionCol[Iterable[String]](json.column)
  case class JSONAllPathsWithTypes(json: ConstOrColMagnet[_]) extends JsonFunctionCol[Map[String, String]](json.column)
  case class JSONDynamicPaths(json: ConstOrColMagnet[_])      extends JsonFunctionCol[Iterable[String]](json.column)
  case class JSONDynamicPathsWithTypes(json: ConstOrColMagnet[_])
      extends JsonFunctionCol[Map[String, String]](json.column)
  case class JSONSharedDataPaths(json: ConstOrColMagnet[_]) extends JsonFunctionCol[Iterable[String]](json.column)
  case class JSONSharedDataPathsWithTypes(json: ConstOrColMagnet[_])
      extends JsonFunctionCol[Map[String, String]](json.column)

  case class DynamicType(dynamic: ConstOrColMagnet[_]) extends JsonFunctionCol[String](dynamic.column)
  case class DynamicElement[V](dynamic: ConstOrColMagnet[_], elementType: ColumnType)
      extends JsonFunctionCol[V](dynamic.column)
  case class IsDynamicElementInSharedData(dynamic: ConstOrColMagnet[_]) extends JsonFunctionCol[Boolean](dynamic.column)

  /** Works on any expression, but takes only a plain path: the `.:`, `.^` and `[]` forms are dot syntax alone. */
  case class GetSubcolumn[V](col: ConstOrColMagnet[_], subcolumn: String) extends JsonFunctionCol[V](col.column)

  // Dot syntax, which the server accepts only after a column name -- `(expr).a` is read as tupleElement, which a JSON
  // value is not -- or after another of these.

  abstract class JSONPathAccess[V](val json: TableColumn[_], val path: Seq[String]) extends JsonFunctionCol[V](json)

  /** `json.a.b`: Dynamic, or the declared type for a typed path. */
  case class JSONSubcolumn[V](override val json: TableColumn[_], override val path: Seq[String])
      extends JSONPathAccess[V](json, path) {
    require(path.nonEmpty, "A JSON subcolumn needs a path")
  }

  /** `json.a.b.:Type`: the path's Dynamic value as `Type`, NULL where it holds another. */
  case class JSONTypedSubcolumn[V](
      override val json: TableColumn[_],
      override val path: Seq[String],
      columnType: ColumnType
  ) extends JSONPathAccess[V](json, path) {
    require(path.nonEmpty, "A JSON subcolumn needs a path")
  }

  /** `json.^a.b`: everything under the path, as JSON. */
  case class JSONSubObject(override val json: TableColumn[_], override val path: Seq[String])
      extends JSONPathAccess[JsValue](json, path) {
    require(path.nonEmpty, "A JSON sub-object needs a path")
  }

  /** `json.a.b[]`, an `Array(JSON)`. With no path it nests: `json.a[][]`. */
  case class JSONArrayOfObjects(override val json: TableColumn[_], override val path: Seq[String])
      extends JSONPathAccess[Iterable[JsValue]](json, path) {
    require(path.nonEmpty || json.isInstanceOf[JSONArrayOfObjects], "An array of JSON objects needs a path")
  }

  def jsonHas(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONHas       = JSONHas(json, path)
  def jsonLength(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONLength = JSONLength(json, path)
  def jsonType(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONType     = JSONType(json, path)
  def jsonKey(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONKey       = JSONKey(json, path)

  def jsonExtractUInt(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractUInt =
    JSONExtractUInt(json, path, caseInsensitive = false)
  def jsonExtractInt(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractInt =
    JSONExtractInt(json, path, caseInsensitive = false)
  def jsonExtractFloat(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractFloat =
    JSONExtractFloat(json, path, caseInsensitive = false)
  def jsonExtractBool(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractBool =
    JSONExtractBool(json, path, caseInsensitive = false)
  def jsonExtractString(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractString =
    JSONExtractString(json, path, caseInsensitive = false)
  def jsonExtractRaw(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractRaw =
    JSONExtractRaw(json, path, caseInsensitive = false)
  def jsonExtractArrayRaw(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractArrayRaw =
    JSONExtractArrayRaw(json, path, caseInsensitive = false)
  def jsonExtractKeys(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractKeys =
    JSONExtractKeys(json, path, caseInsensitive = false)
  def jsonExtractKeysAndValuesRaw(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractKeysAndValuesRaw =
    JSONExtractKeysAndValuesRaw(json, path, caseInsensitive = false)

  /** The server takes the value type last; it comes before the path here because the path is variadic. */
  def jsonExtractKeysAndValues[V](
      json: StringColMagnet[_],
      valueType: ColumnType,
      path: ConstOrColMagnet[_]*
  ): JSONExtractKeysAndValues[V] = JSONExtractKeysAndValues[V](json, valueType, path, caseInsensitive = false)

  /** The server takes the return type last; it comes before the path here because the path is variadic. */
  def jsonExtract[V](json: StringColMagnet[_], returnType: ColumnType, path: ConstOrColMagnet[_]*): JSONExtract[V] =
    JSONExtract[V](json, returnType, path, caseInsensitive = false)

  // The *CaseInsensitive variants need ClickHouse 25.8 or later.

  def jsonExtractUIntCaseInsensitive(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractUInt =
    JSONExtractUInt(json, path, caseInsensitive = true)
  def jsonExtractIntCaseInsensitive(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractInt =
    JSONExtractInt(json, path, caseInsensitive = true)
  def jsonExtractFloatCaseInsensitive(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractFloat =
    JSONExtractFloat(json, path, caseInsensitive = true)
  def jsonExtractBoolCaseInsensitive(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractBool =
    JSONExtractBool(json, path, caseInsensitive = true)
  def jsonExtractStringCaseInsensitive(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractString =
    JSONExtractString(json, path, caseInsensitive = true)
  def jsonExtractRawCaseInsensitive(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractRaw =
    JSONExtractRaw(json, path, caseInsensitive = true)
  def jsonExtractArrayRawCaseInsensitive(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractArrayRaw =
    JSONExtractArrayRaw(json, path, caseInsensitive = true)
  def jsonExtractKeysCaseInsensitive(json: StringColMagnet[_], path: ConstOrColMagnet[_]*): JSONExtractKeys =
    JSONExtractKeys(json, path, caseInsensitive = true)
  def jsonExtractKeysAndValuesRawCaseInsensitive(
      json: StringColMagnet[_],
      path: ConstOrColMagnet[_]*
  ): JSONExtractKeysAndValuesRaw = JSONExtractKeysAndValuesRaw(json, path, caseInsensitive = true)
  def jsonExtractKeysAndValuesCaseInsensitive[V](
      json: StringColMagnet[_],
      valueType: ColumnType,
      path: ConstOrColMagnet[_]*
  ): JSONExtractKeysAndValues[V] = JSONExtractKeysAndValues[V](json, valueType, path, caseInsensitive = true)
  def jsonExtractCaseInsensitive[V](
      json: StringColMagnet[_],
      returnType: ColumnType,
      path: ConstOrColMagnet[_]*
  ): JSONExtract[V] = JSONExtract[V](json, returnType, path, caseInsensitive = true)

  def jsonArrayLength(json: StringColMagnet[_]): JSONArrayLength                          = JSONArrayLength(json)
  def isValidJSON(json: StringColMagnet[_]): IsValidJSON                                  = IsValidJSON(json)
  def jsonMergePatch(json: StringColMagnet[_], more: StringColMagnet[_]*): JSONMergePatch = JSONMergePatch(json, more)

  /** `JSON_EXISTS`. The path is a JSONPath expression such as `$.a.b`. */
  def jsonExists(json: StringColMagnet[_], jsonPath: StringColMagnet[_]): JSONExists = JSONExists(json, jsonPath)

  /** `JSON_QUERY`. The path is a JSONPath expression such as `$.a.b`. */
  def jsonQuery(json: StringColMagnet[_], jsonPath: StringColMagnet[_]): JSONQuery = JSONQuery(json, jsonPath)

  /** `JSON_VALUE`. The path is a JSONPath expression such as `$.a.b`. */
  def jsonValue(json: StringColMagnet[_], jsonPath: StringColMagnet[_]): JSONValue = JSONValue(json, jsonPath)

  def toJSONString(col: ConstOrColMagnet[_]): ToJSONString = ToJSONString(col)

  def jsonAllPaths(json: ConstOrColMagnet[_]): JSONAllPaths                           = JSONAllPaths(json)
  def jsonAllPathsWithTypes(json: ConstOrColMagnet[_]): JSONAllPathsWithTypes         = JSONAllPathsWithTypes(json)
  def jsonDynamicPaths(json: ConstOrColMagnet[_]): JSONDynamicPaths                   = JSONDynamicPaths(json)
  def jsonDynamicPathsWithTypes(json: ConstOrColMagnet[_]): JSONDynamicPathsWithTypes = JSONDynamicPathsWithTypes(json)
  def jsonSharedDataPaths(json: ConstOrColMagnet[_]): JSONSharedDataPaths             = JSONSharedDataPaths(json)
  def jsonSharedDataPathsWithTypes(json: ConstOrColMagnet[_]): JSONSharedDataPathsWithTypes =
    JSONSharedDataPathsWithTypes(json)

  def dynamicType(dynamic: ConstOrColMagnet[_]): DynamicType                                      = DynamicType(dynamic)
  def dynamicElement[V](dynamic: ConstOrColMagnet[_], elementType: ColumnType): DynamicElement[V] =
    DynamicElement[V](dynamic, elementType)
  def isDynamicElementInSharedData(dynamic: ConstOrColMagnet[_]): IsDynamicElementInSharedData =
    IsDynamicElementInSharedData(dynamic)

  def getSubcolumn[V](col: ConstOrColMagnet[_], subcolumn: String): GetSubcolumn[V] = GetSubcolumn[V](col, subcolumn)

  /** One argument per key: `jsonSubcolumn(json, "a", "b")` is `json.a.b`. */
  def jsonSubcolumn[V](json: TableColumn[_], path: String*): JSONSubcolumn[V] = JSONSubcolumn[V](json, path)

  def jsonTypedSubcolumn[V](json: TableColumn[_], columnType: ColumnType, path: String*): JSONTypedSubcolumn[V] =
    JSONTypedSubcolumn[V](json, path, columnType)

  def jsonSubObject(json: TableColumn[_], path: String*): JSONSubObject = JSONSubObject(json, path)

  def jsonArrayOfObjects(json: TableColumn[_], path: String*): JSONArrayOfObjects = JSONArrayOfObjects(json, path)
}
