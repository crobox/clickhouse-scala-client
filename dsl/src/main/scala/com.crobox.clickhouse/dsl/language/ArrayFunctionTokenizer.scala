package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait ArrayFunctionTokenizer { this: ClickhouseTokenizerModule =>

  protected def tokenizeArrayFunction(col: ArrayFunction)(implicit ctx: TokenizeContext): String = col match {
    case col: ArrayFunctionOp[_]    => tokenizeArrayFunctionOp(col)
    case col: ArrayFunctionConst[_] => tokenizeArrayFunctionConst(col)
  }

  protected def tokenizeArrayFunctionOp(col: ArrayFunctionOp[_])(implicit ctx: TokenizeContext): String = col match {
    case EmptyArrayToSingle(col: ArrayColMagnet[_]) =>
      s"emptyArrayToSingle(${tokenizeColumn(col.column)})"
    case Array(columns @ _*) =>
      s"[${tokenizeSeqCol(columns.map(_.column): _*)}]" //Array Creation Operator
    case ArrayConcat(col1: ArrayColMagnet[_], columns @ _*) =>
      s"arrayConcat(${tokenizeSeqCol(col1.column, columns.map(_.column): _*)})"
    case ArrayElement(col: ArrayColMagnet[_], n: NumericCol[_]) =>
      s"${tokenizeColumn(col.column)}[${tokenizeColumn(n.column)}]"
    case Has(col: ArrayColMagnet[_], elm: ConstOrColMagnet[_]) =>
      s"has(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case HasAll(col: ArrayColMagnet[_], elm: ArrayColMagnet[_]) =>
      s"hasAll(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case HasAny(col: ArrayColMagnet[_], elm: ArrayColMagnet[_]) =>
      s"hasAny(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case IndexOf(col: ArrayColMagnet[_], elm: ConstOrColMagnet[_]) =>
      s"indexOf(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case CountEqual(col: ArrayColMagnet[_], elm: ConstOrColMagnet[_]) =>
      s"countEqual(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case ArrayEnumerate(col: ArrayColMagnet[_]) =>
      s"arrayEnumerate(${tokenizeColumn(col.column)})"
    case ArrayEnumerateUniq(col1: ArrayColMagnet[_], columns @ _*) =>
      s"arrayEnumerateUniq(${tokenizeSeqCol(col1.column, columns.map(_.column): _*)})"
    case ArrayPopBack(col: ArrayColMagnet[_]) =>
      s"arrayPopBack(${tokenizeColumn(col.column)})"
    case ArrayPopFront(col: ArrayColMagnet[_]) =>
      s"arrayPopFront(${tokenizeColumn(col.column)})"
    case ArrayPushBack(col: ArrayColMagnet[_], elm: ConstOrColMagnet[_]) =>
      s"arrayPushBack(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case ArrayPushFront(col: ArrayColMagnet[_], elm: ConstOrColMagnet[_]) =>
      s"arrayPushFront(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case ArrayResize(col: ArrayColMagnet[_], size: NumericCol[_], extender: ConstOrColMagnet[_]) =>
      s"arrayResize(${tokenizeColumn(col.column)},${tokenizeColumn(size.column)},${tokenizeColumn(extender.column)})"
    case ArraySlice(col: ArrayColMagnet[_], offset: NumericCol[_], length: NumericCol[_]) =>
      s"arraySlice(${tokenizeColumn(col.column)},${tokenizeColumn(offset.column)},${tokenizeColumn(length.column)})"
    case ArrayUniq(col1: ArrayColMagnet[_], columns @ _*) =>
      s"arrayUniq(${tokenizeSeqCol(col1.column, columns.map(_.column): _*)})"
    case ArrayJoin(col: ArrayColMagnet[_])       => s"arrayJoin(${tokenizeColumn(col.column)})"
    case ArrayDifference(col: ArrayColMagnet[_]) => s"arrayDifference(${tokenizeColumn(col.column)})"
    case ArrayDistinct(col: ArrayColMagnet[_])   => s"arrayDistinct(${tokenizeColumn(col.column)})"
    case ArrayIntersect(col1: ArrayColMagnet[_], columns @ _*) =>
      s"arrayIntersect(${tokenizeSeqCol(col1.column, columns.map(_.column): _*)})"
    case ArrayReduce(function: String, col1: ArrayColMagnet[_], columns @ _*) =>
      s"arrayReduce('$function', ${tokenizeSeqCol(col1.column, columns.map(_.column): _*)})"
  }

  protected def tokenizeArrayFunctionConst(col: ArrayFunctionConst[_])(implicit ctx: TokenizeContext): String =
    col match {
      case _: EmptyArrayUInt8      => "emptyArrayUInt8()"
      case _: EmptyArrayUInt16     => "emptyArrayUInt16()"
      case _: EmptyArrayUInt32     => "emptyArrayUInt32()"
      case _: EmptyArrayUInt64     => "emptyArrayUInt64()"
      case _: EmptyArrayInt8       => "emptyArrayInt8()"
      case _: EmptyArrayInt16      => "emptyArrayInt16()"
      case _: EmptyArrayInt32      => "emptyArrayInt32()"
      case _: EmptyArrayInt64      => "emptyArrayInt64()"
      case _: EmptyArrayFloat32    => "emptyArrayFloat32()"
      case _: EmptyArrayFloat64    => "emptyArrayFloat64()"
      case _: EmptyArrayDate       => "emptyArrayDate()"
      case _: EmptyArrayDateTime   => "emptyArrayDateTime()"
      case _: EmptyArrayString     => "emptyArrayString()"
      case Range(n: NumericCol[_]) => s"range(${tokenizeColumn(n.column)})"
    }
}
