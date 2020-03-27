package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.dsl._

trait MiscellaneousFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeMiscellaneousFunction(col: MiscellaneousFunction): String = col match {
    case col: MiscellaneousOp[_]    => tokenizeMiscOp(col)
    case col: MiscellaneousConst[_] => tokenizeMiscConst(col)
  }

  def tokenizeMiscOp(col: MiscellaneousOp[_]): String = col match {
    case VisibleWidth(col: ConstOrColMagnet[_]) => s"visibleWidth(${tokenizeColumn(col.column)})"
    case ToTypeName(col: ConstOrColMagnet[_])   => s"toTypeName(${tokenizeColumn(col.column)})"
    case Materialize(col: ConstOrColMagnet[_])  => s"materialize(${tokenizeColumn(col.column)})"
    case Sleep(col: NumericCol[_])              => s"sleep(${tokenizeColumn(col.column)})"
    case IsFinite(col: NumericCol[_])           => s"isFinite(${tokenizeColumn(col.column)})"
    case IsInfinite(col: NumericCol[_])         => s"isInfinite(${tokenizeColumn(col.column)})"
    case IsNaN(col: NumericCol[_])              => s"isNaN(${tokenizeColumn(col.column)})"
    case Bar(col: NumericCol[_], from: NumericCol[_], to: NumericCol[_], default: Option[NumericCol[_]]) => {
      val defaultPart = default.map(col => "," + tokenizeColumn(col.column)).getOrElse("")
      s"bar(${tokenizeColumn(col.column)},${tokenizeColumn(from.column)},${tokenizeColumn(to.column)}${defaultPart})"
    }
    case Transform(col: ConstOrColMagnet[_],
                   arrayFrom: ArrayColMagnet[_],
                   arrayTo: ArrayColMagnet[_],
                   default: ConstOrColMagnet[_]) =>
      s"transform(${tokenizeColumn(col.column)},${tokenizeColumn(arrayFrom.column)},${tokenizeColumn(arrayTo.column)},${tokenizeColumn(default.column)})"
    case FormatReadableSize(col: NumericCol[_]) => s"formatReadableSize(${tokenizeColumn(col.column)})"
    case Least(a: ConstOrColMagnet[_], b: ConstOrColMagnet[_]) =>
      s"least(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
    case Greatest(a: ConstOrColMagnet[_], b: ConstOrColMagnet[_]) =>
      s"greatest(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
    case RunningDifference(col: ConstOrColMagnet[_]) => s"runningDifference(${tokenizeColumn(col.column)})"
    case MACNumToString(col: NumericCol[_])          => s"MACNumToString(${tokenizeColumn(col.column)})"
    case MACStringToNum(col: StringColMagnet[_])     => s"MACStringToNum(${tokenizeColumn(col.column)})"
    case MACStringToOUI(col: StringColMagnet[_])     => s"MACStringToOUI(${tokenizeColumn(col.column)})"
  }

  def tokenizeMiscConst(const: MiscellaneousConst[_]): String = const match {
    case HostName() =>
      "hostName()"
    case BlockSize() =>
      "blockSize()"
    case Ignore(columns @ _*) =>
      s"ignore(${tokenizeSeqCol(columns.map(_.column): _*)})"
    case CurrentDatabase() =>
      "currentDatabase()"
    case HasColumnInTable(database, table, column, hostName, userName, passWord) =>
      s"hasColumnInTable(${tokenizeColumn(database.column)},${tokenizeColumn(table.column)},${tokenizeColumn(
        column.column
      )}${tokenizeOpt(hostName)}${tokenizeOpt(userName)}${tokenizeOpt(passWord)})"
    case Uptime()               => "uptime()"
    case Version()              => "version()"
    case RowNumberInAllBlocks() => "rowNumberInAllBlocks()"
  }

  private def tokenizeOpt(col: Option[Magnet[_]]) = col match {
    case Some(magnetizedCol) => "," + tokenizeColumn(magnetizedCol.column)
    case _                   => ""
  }
}
