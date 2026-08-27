package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait ArrayFunctionTokenizer { this: ClickhouseTokenizerModule =>

  protected def tokenizeArrayFunction(col: ArrayFunction)(implicit ctx: TokenizeContext): String = col match {
    case col: ArrayFunctionOp[_]    => tokenizeArrayFunctionOp(col)
    case col: ArrayFunctionConst[_] => tokenizeArrayFunctionConst(col)
  }

  protected def tokenizeArrayFunctionOp(col: ArrayFunctionOp[_])(implicit ctx: TokenizeContext): String = col match {
    case EmptyArrayToSingle(col) =>
      s"emptyArrayToSingle(${tokenizeColumn(col.column)})"
    case Array(columns @ _*) =>
      s"[${tokenizeSeqCol(columns.map(_.column): _*)}]" // Array Creation Operator
    case ArrayConcat(col1, columns @ _*) =>
      s"arrayConcat(${tokenizeSeqCol(col1.column, columns.map(_.column): _*)})"
    case ArrayElement(col, n) =>
      s"${tokenizeColumn(col.column)}[${tokenizeColumn(n.column)}]"
    case Has(col, elm) =>
      s"has(${tokenizeColumn(col.column)}, ${tokenizeColumn(elm.column)})"
    case HasAll(col, elm) =>
      s"hasAll(${tokenizeColumn(col.column)}, ${tokenizeColumn(elm.column)})"
    case HasAny(col, elm) =>
      s"hasAny(${tokenizeColumn(col.column)}, ${tokenizeColumn(elm.column)})"
    case IndexOf(col, elm) =>
      s"indexOf(${tokenizeColumn(col.column)}, ${tokenizeColumn(elm.column)})"
    case CountEqual(col, elm) =>
      s"countEqual(${tokenizeColumn(col.column)}, ${tokenizeColumn(elm.column)})"
    case ArrayEnumerate(col) =>
      s"arrayEnumerate(${tokenizeColumn(col.column)})"
    case ArrayEnumerateUniq(col1, columns @ _*) =>
      s"arrayEnumerateUniq(${tokenizeSeqCol(col1.column, columns.map(_.column): _*)})"
    case ArrayPopBack(col) =>
      s"arrayPopBack(${tokenizeColumn(col.column)})"
    case ArrayPopFront(col) =>
      s"arrayPopFront(${tokenizeColumn(col.column)})"
    case ArrayPushBack(col, elm) =>
      s"arrayPushBack(${tokenizeColumn(col.column)}, ${tokenizeColumn(elm.column)})"
    case ArrayPushFront(col, elm) =>
      s"arrayPushFront(${tokenizeColumn(col.column)}, ${tokenizeColumn(elm.column)})"
    case ArrayResize(col, size, extender) =>
      s"arrayResize(${tokenizeColumn(col.column)},${tokenizeColumn(size.column)},${tokenizeColumn(extender.column)})"
    case ArraySlice(col, offset, length) =>
      s"arraySlice(${tokenizeColumn(col.column)},${tokenizeColumn(offset.column)},${tokenizeColumn(length.column)})"
    case ArrayUniq(col1, columns @ _*) =>
      s"arrayUniq(${tokenizeSeqCol(col1.column, columns.map(_.column): _*)})"
    case ArrayJoin(col)                     => s"arrayJoin(${tokenizeColumn(col.column)})"
    case ArrayDifference(col)               => s"arrayDifference(${tokenizeColumn(col.column)})"
    case ArrayDistinct(col)                 => s"arrayDistinct(${tokenizeColumn(col.column)})"
    case ArrayIntersect(col1, columns @ _*) =>
      s"arrayIntersect(${tokenizeSeqCol(col1.column, columns.map(_.column): _*)})"
    case ArrayReduce(function, col1, columns @ _*) =>
      s"arrayReduce('$function', ${tokenizeSeqCol(col1.column, columns.map(_.column): _*)})"
    case ArrayReverse(col)  => s"arrayReverse(${tokenizeColumn(col.column)})"
    case ArrayEmpty(col)    => s"empty(${tokenizeColumn(col.column)})"
    case ArrayNotEmpty(col) => s"notEmpty(${tokenizeColumn(col.column)})"
    case ArrayLength(col)   => s"length(${tokenizeColumn(col.column)})"
    case ArrayFlatten(col)  => s"arrayFlatten(${tokenizeColumn(col.column)})"
  }

  protected def tokenizeArrayFunctionConst(col: ArrayFunctionConst[_])(implicit ctx: TokenizeContext): String =
    col match {
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
      case Range(n)              => s"range(${tokenizeColumn(n.column)})"
    }
}
