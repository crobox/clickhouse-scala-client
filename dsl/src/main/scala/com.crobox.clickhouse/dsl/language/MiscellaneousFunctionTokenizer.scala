package com.crobox.clickhouse.dsl.language

import com.dongxiguo.fastring.Fastring.Implicits._
import com.crobox.clickhouse.dsl._

trait MiscellaneousFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeMiscellaneousFunction(col: MiscellaneousFunction): String = col match {
    case col: MiscellaneousOp[_]    => tokenizeMiscOp(col)
    case col: MiscellaneousConst[_] => tokenizeMiscConst(col)
  }

  def tokenizeMiscOp(col: MiscellaneousOp[_]): String = col match {
    case VisibleWidth(col: ConstOrColMagnet) => fast"visibleWidth(${tokenizeColumn(col.column)})"
    case ToTypeName(col: ConstOrColMagnet)   => fast"toTypeName(${tokenizeColumn(col.column)})"
    case Materialize(col: ConstOrColMagnet)  => fast"materialize(${tokenizeColumn(col.column)})"
    case Sleep(col: NumericCol)              => fast"sleep(${tokenizeColumn(col.column)})"
    case IsFinite(col: NumericCol)           => fast"isFinite(${tokenizeColumn(col.column)})"
    case IsInfinite(col: NumericCol)         => fast"isInfinite(${tokenizeColumn(col.column)})"
    case IsNaN(col: NumericCol)              => fast"isNaN(${tokenizeColumn(col.column)})"
    case Bar(col: ConstOrColMagnet)          => fast"bar(${tokenizeColumn(col.column)})"
    case Transform(col: ConstOrColMagnet,
                   arrayFrom: ArrayColMagnet,
                   arrayTo: ArrayColMagnet,
                   default: ConstOrColMagnet) =>
      fast"transform(${tokenizeColumn(col.column)},${tokenizeColumn(arrayFrom.column)},${tokenizeColumn(arrayTo.column)},${tokenizeColumn(default.column)})"
    case FormatReadableSize(col: NumericCol) => fast"formatReadableSize(${tokenizeColumn(col.column)})"
    case Least(a: ConstOrColMagnet, b: ConstOrColMagnet) =>
      fast"least(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
    case Greatest(a: ConstOrColMagnet, b: ConstOrColMagnet) =>
      fast"greatest(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
    case RunningDifference(col: ConstOrColMagnet) => fast"runningDifference(${tokenizeColumn(col.column)})"
    case MACNumToString(col: NumericCol)          => fast"mACNumToString(${tokenizeColumn(col.column)})"
    case MACStringToNum(col: StringColMagnet)     => fast"mACStringToNum(${tokenizeColumn(col.column)})"
    case MACStringToOUI(col: StringColMagnet)     => fast"mACStringToOUI(${tokenizeColumn(col.column)})"
  }

  def tokenizeMiscConst(const: MiscellaneousConst[_]): String = const match {
    case HostName()                          => "hostName()"
    case BlockSize()                         => "blockSize()"
    case Ignore(coln: Seq[ConstOrColMagnet]) => fast"ignore(${tokenizeSeqCol(coln.map(_.column))})"
    case CurrentDatabase()                   => "currentDatabase()"
    case HasColumnInTable(database, table, column, hostName, userName, passWord) =>
      fast"hasColumnInTable(${tokenizeColumn(database.column)},${tokenizeColumn(table.column)},${tokenizeColumn(
        column.column
      )}${tokenizeOpt(hostName)}${tokenizeOpt(userName)}${tokenizeOpt(passWord)})"
    case Uptime()               => "uptime()"
    case Version()              => "version()"
    case RowNumberInAllBlocks() => "rowNumberInAllBlocks()"
  }

  private def tokenizeOpt(col: Option[Magnet]) = col match {
    case Some(magnetizedCol) => "," + tokenizeColumn(magnetizedCol.column)
    case _                   => ""
  }
}
