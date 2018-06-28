package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait DictionaryFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  private def tokenizeDictionaryGet(col: DictionaryGetFuncColumn[_], typeName: String) = {
    val default = col.default
      .map(col => "," + tokenizeColumn(col))
      .getOrElse("")

    fast"dictGet$typeName(${tokenizeColumn(col.dictName.column)},${tokenizeColumn(col.attrName.column)},${tokenizeColumn(col.id.column)}$default)"
  }

  def tokenizeDictionaryFunction(col: DictionaryFuncColumn[_]): String = col match {
    case col: DictGetUInt8                                                                => tokenizeDictionaryGet(col, "UInt8")
    case col: DictGetUInt16                                                               => tokenizeDictionaryGet(col, "UInt16")
    case col: DictGetUInt32                                                               => tokenizeDictionaryGet(col, "UInt32")
    case col: DictGetUInt64                                                               => tokenizeDictionaryGet(col, "UInt64")
    case col: DictGetInt8                                                                 => tokenizeDictionaryGet(col, "Int8")
    case col: DictGetInt16                                                                => tokenizeDictionaryGet(col, "Int16")
    case col: DictGetInt32                                                                => tokenizeDictionaryGet(col, "Int32")
    case col: DictGetInt64                                                                => tokenizeDictionaryGet(col, "Int64")
    case col: DictGetFloat32                                                              => tokenizeDictionaryGet(col, "Float32")
    case col: DictGetFloat64                                                              => tokenizeDictionaryGet(col, "Float64")
    case col: DictGetDate                                                                 => tokenizeDictionaryGet(col, "Date")
    case col: DictGetDateTime                                                             => tokenizeDictionaryGet(col, "DateTime")
    case col: DictGetUUID                                                                 => tokenizeDictionaryGet(col, "UUID")
    case col: DictGetString                                                               => tokenizeDictionaryGet(col, "String")
    case DictIsIn(dictName: StringColMagnet, childId: NumericCol, ancestorId: NumericCol) =>
      fast"dictIsIn(${tokenizeColumn(dictName.column)},${tokenizeColumn(childId.column)},${tokenizeColumn(ancestorId.column)})"
    case DictGetHierarchy(dictName: StringColMagnet, id: NumericCol)                      =>
      fast"dictGetHierarchy(${tokenizeColumn(dictName.column)},${tokenizeColumn(id.column)})"
    case DictHas(dictName: StringColMagnet, id: NumericCol)                               => 
      fast"dictHas(${tokenizeColumn(dictName.column)},${tokenizeColumn(id.column)})"
  }
}
