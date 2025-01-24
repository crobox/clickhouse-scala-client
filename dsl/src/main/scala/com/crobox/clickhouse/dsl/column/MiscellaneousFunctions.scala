package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

trait MiscellaneousFunctions { self: Magnets =>
  sealed trait MiscellaneousFunction

  abstract class MiscellaneousOp[V](col: ConstOrColMagnet[_])
      extends ExpressionColumn[V](col.column)
      with MiscellaneousFunction
  abstract class MiscellaneousConst[V]() extends ExpressionColumn[V](EmptyColumn) with MiscellaneousFunction

  case class HostName()                             extends MiscellaneousConst[String]()
  case class VisibleWidth(col: ConstOrColMagnet[_]) extends MiscellaneousOp[Long](col)
  case class ToTypeName(col: ConstOrColMagnet[_])   extends MiscellaneousOp[String](col)
  case class BlockSize()                            extends MiscellaneousConst[Long]()
  case class Materialize(col: ConstOrColMagnet[_])  extends MiscellaneousOp[Long](col)
  case class Ignore(coln: ConstOrColMagnet[_]*)     extends MiscellaneousConst[Long]()
  case class Sleep(col: NumericCol[_])              extends MiscellaneousOp[Long](col.column) // is this an operator?
  case class CurrentDatabase()                      extends MiscellaneousConst[String]()
  case class IsFinite(col: NumericCol[_])           extends MiscellaneousOp[Boolean](col.column)
  case class IsInfinite(col: NumericCol[_])         extends MiscellaneousOp[Boolean](col.column)
  case class IsNaN(col: NumericCol[_])              extends MiscellaneousOp[Boolean](col.column)
  case class HasColumnInTable(
      database: StringColMagnet[_],
      table: StringColMagnet[_],
      column: StringColMagnet[_],
      hostName: Option[StringColMagnet[_]] = None,
      userName: Option[StringColMagnet[_]] = None,
      passWord: Option[StringColMagnet[_]] = None
  ) extends MiscellaneousConst[Boolean]()
  case class Bar(col: NumericCol[_], from: NumericCol[_], to: NumericCol[_], default: Option[NumericCol[_]])
      extends MiscellaneousOp[String](col.column)
  case class Transform[L, R](
      col: ConstOrColMagnet[L],
      arrayFrom: ArrayColMagnet[Iterable[L]],
      arrayTo: ArrayColMagnet[Iterable[R]],
      default: ConstOrColMagnet[R]
  ) extends MiscellaneousOp[Long](col)
  case class FormatReadableSize(col: NumericCol[_])                   extends MiscellaneousOp[String](col.column)
  case class Least(a: ConstOrColMagnet[_], b: ConstOrColMagnet[_])    extends MiscellaneousOp[Long](a)
  case class Greatest(a: ConstOrColMagnet[_], b: ConstOrColMagnet[_]) extends MiscellaneousOp[Long](a)
  case class Uptime()                                                 extends MiscellaneousConst[Long]()
  case class Version()                                                extends MiscellaneousConst[Long]()
  case class RowNumberInAllBlocks()                                   extends MiscellaneousConst[Long]()
  case class RunningDifference(col: ConstOrColMagnet[_])              extends MiscellaneousOp[Long](col)
  case class MACNumToString(col: NumericCol[_])                       extends MiscellaneousOp[String](col.column)
  case class MACStringToNum(col: StringColMagnet[_])                  extends MiscellaneousOp[Long](col.column)
  case class MACStringToOUI(col: StringColMagnet[_])                  extends MiscellaneousOp[Long](col.column)

  def hostName()                             = HostName()
  def visibleWidth(col: ConstOrColMagnet[_]) = VisibleWidth(col)
  def toTypeName(col: ConstOrColMagnet[_])   = ToTypeName(col)
  def blockSize()                            = BlockSize()
  def materialize(col: ConstOrColMagnet[_])  = Materialize(col)
  def ignore(coln: ConstOrColMagnet[_]*)     = Ignore(coln: _*)
  def sleep(col: NumericCol[_])              = Sleep(col: NumericCol[_])
  def currentDatabase()                      = CurrentDatabase()
  def isFinite[O](col: NumericCol[O])        = IsFinite(col)
  def isInfinite(col: NumericCol[_])         = IsInfinite(col)
  def isNaN(col: NumericCol[_])              = IsNaN(col: NumericCol[_])

  def hasColumnInTable(
      database: StringColMagnet[_],
      table: StringColMagnet[_],
      column: StringColMagnet[_],
      hostName: Option[StringColMagnet[_]] = None,
      userName: Option[StringColMagnet[_]] = None,
      passWord: Option[StringColMagnet[_]] = None
  ) =
    HasColumnInTable(database, table, column, hostName, userName, passWord)

  def bar(col: NumericCol[_], from: NumericCol[_], to: NumericCol[_], default: Option[NumericCol[_]]) =
    Bar(col, from, to, default)

  def transform[L, R](
      col: ConstOrColMagnet[L],
      arrayFrom: ArrayColMagnet[Iterable[L]],
      arrayTo: ArrayColMagnet[Iterable[R]],
      default: ConstOrColMagnet[R]
  ) =
    Transform[L, R](col, arrayFrom, arrayTo, default)
  def formatReadableSize(col: NumericCol[_])                   = FormatReadableSize(col)
  def least(a: ConstOrColMagnet[_], b: ConstOrColMagnet[_])    = Least(a: ConstOrColMagnet[_], b)
  def greatest(a: ConstOrColMagnet[_], b: ConstOrColMagnet[_]) = Greatest(a: ConstOrColMagnet[_], b)
  def uptime()                                                 = Uptime()
  def version()                                                = Version()
  def rowNumberInAllBlocks()                                   = RowNumberInAllBlocks()
  def runningDifference(col: ConstOrColMagnet[_])              = RunningDifference(col)
  def mACNumToString(col: NumericCol[_])                       = MACNumToString(col)
  def mACStringToNum(col: StringColMagnet[_])                  = MACStringToNum(col)
  def mACStringToOUI(col: StringColMagnet[_])                  = MACStringToOUI(col)
  /*

  hostName()
  visibleWidth(x)
  toTypeName(x)
  blockSize()
  materialize(x)
  ignore(...)
  sleep(seconds)
  currentDatabase()
  isFinite(x)
  isInfinite(x)
  isNaN(x)
  hasColumnInTable(['hostname'[, 'username'[, 'password']],] 'database', 'table', 'column')
  bar
  transform
  formatReadableSize(x)
  least(a, b)
  greatest(a, b)
  uptime()
  version()
  rowNumberInAllBlocks()
  runningDifference(x)
  MACNumToString(num)
  MACStringToNum(s)
  MACStringToOUI(s)

  arrayJoin
  tuple
   */
}
