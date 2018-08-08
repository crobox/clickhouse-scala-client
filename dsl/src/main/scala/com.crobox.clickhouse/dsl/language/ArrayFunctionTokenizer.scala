package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl.TableColumn.AnyTableColumn

import com.crobox.clickhouse.dsl._
import com.dongxiguo.fastring.Fastring.Implicits._

trait ArrayFunctionTokenizer { this: ClickhouseTokenizerModule =>
  protected def tokenizeArrayFunction(col: ArrayFunction) = col match {
    case col: ArrayFunctionOp[_]    => tokenizeArrayFunctionOp(col)
    case col: ArrayFunctionConst[_] => tokenizeArrayFunctionConst(col)
  }

  protected def tokenizeArrayFunctionOp(col: ArrayFunctionOp[_]): String = col match {
    case EmptyArrayToSingle(col: ArrayColMagnet[_]) => fast"emptyArrayToSingle(${tokenizeColumn(col.column)})"
    case Array(col1: ConstOrColMagnet[_], coln@_*) =>
      fast"array(${tokenizeColumn(col1.column)}${tokenizeSeqCol(coln.map(_.column))})"
    case ArrayConcat(col1: ArrayColMagnet[_], col2: ArrayColMagnet[_], coln@_*) =>
      fast"arrayConcat(${tokenizeColumn(col1.column)},${tokenizeColumn(col2.column)}${tokenizeSeqCol(coln.map(_.column))})"
    case ArrayElement(col: ArrayColMagnet[_], n: NumericCol[_]) =>
      fast"arrayElement(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case Has(col: ArrayColMagnet[_], elm:  ConstOrColMagnet[_]) => fast"has(${tokenizeColumn(col.column)}${tokenizeColumn(elm.column)})"
    case IndexOf(col: ArrayColMagnet[_], elm:  ConstOrColMagnet[_]) =>
      fast"indexOf(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case CountEqual(col: ArrayColMagnet[_], elm:  ConstOrColMagnet[_]) =>
      fast"countEqual(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case ArrayEnumerate(col: ArrayColMagnet[_]) => fast"arrayEnumerate(${tokenizeColumn(col.column)})"
    case ArrayEnumerateUniq(col1: ArrayColMagnet[_], coln@_*) =>
      fast"arrayEnumerateUniq(${tokenizeColumn(col1.column)}${tokenizeSeqCol(coln.map(_.column))})"
    case ArrayPopBack(col: ArrayColMagnet[_])  => fast"arrayPopBack(${tokenizeColumn(col.column)})"
    case ArrayPopFront(col: ArrayColMagnet[_]) => fast"arrayPopFront(${tokenizeColumn(col.column)})"
    case ArrayPushBack(col: ArrayColMagnet[_], elm:  ConstOrColMagnet[_]) =>
      fast"arrayPushBack(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case ArrayPushFront(col: ArrayColMagnet[_], elm: ConstOrColMagnet[_]) =>
      fast"arrayPushFront(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case ArraySlice(col: ArrayColMagnet[_], offset: NumericCol[_], length: NumericCol[_]) =>
      fast"arraySlice(${tokenizeColumn(col.column)},${tokenizeColumn(offset.column)},${tokenizeColumn(length.column)})"
    case ArrayUniq(col1: ArrayColMagnet[_], coln@_*) =>
      fast"arrayUniq(${tokenizeColumn(col1.column)}${tokenizeSeqCol(coln.map(_.column))})"
    case ArrayJoin(col: ArrayColMagnet[_]) => fast"arrayJoin(${tokenizeColumn(col.column)})"
  }

  protected def tokenizeArrayFunctionConst(col: ArrayFunctionConst[_]): String = col match {
    case _: EmptyArrayUInt8    => "emptyArrayUInt8()"
    case _: EmptyArrayUInt16   => "emptyArrayUInt16()"
    case _: EmptyArrayUInt32   => "emptyArrayUInt32()"
    case _: EmptyArrayUInt64   => "emptyArrayUInt64()"
    case _: EmptyArrayInt8     => "emptyArrayInt8()"
    case _: EmptyArrayInt16    => "emptyArrayInt16()"
    case _: EmptyArrayInt32    => "emptyArrayInt32()"
    case _: EmptyArrayInt64    => "emptyArrayInt64()"
    case _: EmptyArrayFloat32  => "emptyArrayFloat32()"
    case _: EmptyArrayFloat64  => "emptyArrayFloat64()"
    case _: EmptyArrayDate     => "emptyArrayDate()"
    case _: EmptyArrayDateTime => "emptyArrayDateTime()"
    case _: EmptyArrayString   => "emptyArrayString()"
    case Range(n: NumericCol[_])  => fast"range(${tokenizeColumn(n.column)})"
  }
}
