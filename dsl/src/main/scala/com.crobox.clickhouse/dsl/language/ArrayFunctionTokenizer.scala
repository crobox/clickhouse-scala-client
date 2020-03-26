package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait ArrayFunctionTokenizer { this: ClickhouseTokenizerModule =>
  protected def tokenizeArrayFunction(col: ArrayFunction): String = col match {
    case col: ArrayFunctionOp[_]    => tokenizeArrayFunctionOp(col)
    case col: ArrayFunctionConst[_] => tokenizeArrayFunctionConst(col)
  }

  protected def tokenizeArrayFunctionOp(col: ArrayFunctionOp[_]): String = col match {
    case EmptyArrayToSingle(col: ArrayColMagnet[_]) => s"emptyArrayToSingle(${tokenizeColumn(col.column)})"
    case Array(col1: ConstOrColMagnet[_], coln @ _*) =>
      s"[${tokenizeColumn(col1.column)}${tokenizeSeqCol(coln.map(_.column))}]" //Array Creation Operator
    case ArrayConcat(col1: ArrayColMagnet[_], col2: ArrayColMagnet[_], coln @ _*) =>
      s"arrayConcat(${tokenizeColumn(col1.column)},${tokenizeColumn(col2.column)}${tokenizeSeqCol(coln.map(_.column))})"
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
    case ArrayEnumerate(col: ArrayColMagnet[_]) => s"arrayEnumerate(${tokenizeColumn(col.column)})"
    case ArrayEnumerateUniq(col1: ArrayColMagnet[_], coln @ _*) =>
      s"arrayEnumerateUniq(${tokenizeColumn(col1.column)}${tokenizeSeqCol(coln.map(_.column))})"
    case ArrayPopBack(col: ArrayColMagnet[_])  => s"arrayPopBack(${tokenizeColumn(col.column)})"
    case ArrayPopFront(col: ArrayColMagnet[_]) => s"arrayPopFront(${tokenizeColumn(col.column)})"
    case ArrayPushBack(col: ArrayColMagnet[_], elm: ConstOrColMagnet[_]) =>
      s"arrayPushBack(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case ArrayPushFront(col: ArrayColMagnet[_], elm: ConstOrColMagnet[_]) =>
      s"arrayPushFront(${tokenizeColumn(col.column)},${tokenizeColumn(elm.column)})"
    case ArrayResize(col: ArrayColMagnet[_], size: NumericCol[_], extender: ConstOrColMagnet[_]) =>
      s"arrayResize(${tokenizeColumn(col.column)},${tokenizeColumn(size.column)},${tokenizeColumn(extender.column)})"
    case ArraySlice(col: ArrayColMagnet[_], offset: NumericCol[_], length: NumericCol[_]) =>
      s"arraySlice(${tokenizeColumn(col.column)},${tokenizeColumn(offset.column)},${tokenizeColumn(length.column)})"
    case ArrayUniq(col1: ArrayColMagnet[_], coln @ _*) =>
      s"arrayUniq(${tokenizeColumn(col1.column)}${tokenizeSeqCol(coln.map(_.column))})"
    case ArrayJoin(col: ArrayColMagnet[_]) => s"arrayJoin(${tokenizeColumn(col.column)})"
  }

  protected def tokenizeArrayFunctionConst(col: ArrayFunctionConst[_]): String = col match {
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
