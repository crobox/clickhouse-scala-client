package com.crobox.clickhouse.dsl.language

import com.dongxiguo.fastring.Fastring.Implicits._
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.language.TokenizerModule.Database

trait MiscellaneousFunctionTokenizer {
  self: ClickhouseTokenizerModule =>

  def tokenizeMiscellaneousFunction(col: MiscellaneousFunction)(implicit database: Database): String = col match {
    case col: MiscellaneousOp[_]    => tokenizeMiscOp(col)
    case col: MiscellaneousConst[_] => tokenizeMiscConst(col)
  }

  def tokenizeMiscOp(col: MiscellaneousOp[_])(implicit database: Database): String = col match {
    case VisibleWidth(col: ConstOrColMagnet[_]) => fast"visibleWidth(${tokenizeColumn(col.column)})"
    case ToTypeName(col: ConstOrColMagnet[_])   => fast"toTypeName(${tokenizeColumn(col.column)})"
    case Materialize(col: ConstOrColMagnet[_])  => fast"materialize(${tokenizeColumn(col.column)})"
    case Sleep(col: NumericCol[_])              => fast"sleep(${tokenizeColumn(col.column)})"
    case IsFinite(col: NumericCol[_])           => fast"isFinite(${tokenizeColumn(col.column)})"
    case IsInfinite(col: NumericCol[_])         => fast"isInfinite(${tokenizeColumn(col.column)})"
    case IsNaN(col: NumericCol[_])              => fast"isNaN(${tokenizeColumn(col.column)})"
    case Bar(col: NumericCol[_],from: NumericCol[_],to: NumericCol[_], default: Option[NumericCol[_]]) => {
      val defaultPart = default.map(col => "," + tokenizeColumn(col.column)).getOrElse("")
      fast"bar(${tokenizeColumn(col.column)},${tokenizeColumn(from.column)},${tokenizeColumn(to.column)}${defaultPart})"
    }
    case Transform(col: ConstOrColMagnet[_],
                   arrayFrom: ArrayColMagnet[_],
                   arrayTo: ArrayColMagnet[_],
                   default: ConstOrColMagnet[_]) =>
      fast"transform(${tokenizeColumn(col.column)},${tokenizeColumn(arrayFrom.column)},${tokenizeColumn(arrayTo.column)},${tokenizeColumn(default.column)})"
    case FormatReadableSize(col: NumericCol[_]) => fast"formatReadableSize(${tokenizeColumn(col.column)})"
    case Least(a: ConstOrColMagnet[_], b: ConstOrColMagnet[_]) =>
      fast"least(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
    case Greatest(a: ConstOrColMagnet[_], b: ConstOrColMagnet[_]) =>
      fast"greatest(${tokenizeColumn(a.column)},${tokenizeColumn(b.column)})"
    case RunningDifference(col: ConstOrColMagnet[_]) => fast"runningDifference(${tokenizeColumn(col.column)})"
    case MACNumToString(col: NumericCol[_])          => fast"MACNumToString(${tokenizeColumn(col.column)})"
    case MACStringToNum(col: StringColMagnet[_])     => fast"MACStringToNum(${tokenizeColumn(col.column)})"
    case MACStringToOUI(col: StringColMagnet[_])     => fast"MACStringToOUI(${tokenizeColumn(col.column)})"
  }

  def tokenizeMiscConst(const: MiscellaneousConst[_])(implicit db: Database): String = const match {
    case HostName()                          => "hostName()"
    case BlockSize()                         => "blockSize()"
    case Ignore(coln @ _*) =>
      fast"ignore(${tokenizeSeqCol(coln.map(_.column))})"

    case CurrentDatabase()                   => "currentDatabase()"
    case HasColumnInTable(database, table, column, hostName, userName, passWord) =>
      fast"hasColumnInTable(${tokenizeColumn(database.column)},${tokenizeColumn(table.column)},${tokenizeColumn(
        column.column
      )}${tokenizeOpt(hostName)}${tokenizeOpt(userName)}${tokenizeOpt(passWord)})"
    case Uptime()               => "uptime()"
    case Version()              => "version()"
    case RowNumberInAllBlocks() => "rowNumberInAllBlocks()"
  }

  private def tokenizeOpt(col: Option[Magnet[_]])(implicit database: Database) = col match {
    case Some(magnetizedCol) => "," + tokenizeColumn(magnetizedCol.column)
    case _                   => ""
  }
}
