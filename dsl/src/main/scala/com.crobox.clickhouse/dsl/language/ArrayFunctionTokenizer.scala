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
    case EmptyArrayToSingle(col: ArrayColMagnet) => fast"emptyArrayToSingle(${tokenizeColumn(col.column)})"
    case Array(col1: ArrayColMagnet, coln: Seq[ArrayColMagnet]) =>
      fast"array(${tokenizeColumn(col)}${tokenizeSeqCol(coln.map(_.column))})"
    case ArrayConcat(col1: ArrayColMagnet, col2: ArrayColMagnet, coln: Seq[ArrayColMagnet]) =>
      fast"arrayConcat(${tokenizeColumn(col)}${tokenizeColumn(col2.column)}${tokenizeSeqCol(coln.map(_.column))})"
    case ArrayElement(col: ArrayColMagnet, n: NumericCol) =>
      fast"arrayElement(${tokenizeColumn(col.column)},${tokenizeColumn(n.column)})"
    case Has(col: ArrayColMagnet, elm: AnyTableColumn) => fast"has(${tokenizeColumn(col.column)}${tokenizeColumn(elm)})"
    case IndexOf(col: ArrayColMagnet, elm: AnyTableColumn) =>
      fast"indexOf(${tokenizeColumn(col.column)}${tokenizeColumn(elm)})"
    case CountEqual(col: ArrayColMagnet, elm: AnyTableColumn) =>
      fast"countEqual(${tokenizeColumn(col.column)}${tokenizeColumn(elm)})"
    case ArrayEnumerate(col: ArrayColMagnet) => fast"arrayEnumerate(${tokenizeColumn(col.column)})"
    case ArrayEnumerateUniq(col1: ArrayColMagnet, coln: Seq[ArrayColMagnet]) =>
      fast"arrayEnumerateUniq(${tokenizeColumn(col)}${tokenizeSeqCol(coln.map(_.column))})"
    case ArrayPopBack(col: ArrayColMagnet)  => fast"arrayPopBack(${tokenizeColumn(col.column)})"
    case ArrayPopFront(col: ArrayColMagnet) => fast"arrayPopFront(${tokenizeColumn(col.column)})"
    case ArrayPushBack(col: ArrayColMagnet, elm: AnyTableColumn) =>
      fast"arrayPushBack(${tokenizeColumn(col.column)}${tokenizeColumn(elm)})"
    case ArrayPushFront(col: ArrayColMagnet, elm: AnyTableColumn) =>
      fast"arrayPushFront(${tokenizeColumn(col.column)}${tokenizeColumn(elm)})"
    case ArraySlice(col: ArrayColMagnet, offset: NumericCol, length: NumericCol) =>
      fast"arraySlice(${tokenizeColumn(col.column)}${tokenizeColumn(offset.column)}${tokenizeColumn(length.column)})"
    case ArrayUniq(col1: ArrayColMagnet, coln: Seq[ArrayColMagnet]) =>
      fast"arrayUniq(${tokenizeColumn(col)}${tokenizeSeqCol(coln.map(_.column))})"
    case ArrayJoin(col: ArrayColMagnet) => fast"arrayJoin(${tokenizeColumn(col.column)})"
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
    case Range(n: NumericCol)  => fast"range(${tokenizeColumn(n.column)})"
  }
}
