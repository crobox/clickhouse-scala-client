package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.dsl._

object LogicalOperatorImprovements {

  implicit class TableColumnImpr(left: TableColumn[Boolean]) {

    def and(right: Option[TableColumn[Boolean]]): TableColumn[Boolean] = right match {
      case Some(condition) => LogicalFunction(left, And, condition)
      case _               => left
    }

    def and(right: TableColumn[Boolean]): TableColumn[Boolean] = LogicalFunction(left, And, right)

    def or(right: Option[TableColumn[Boolean]]): TableColumn[Boolean] = right match {
      case Some(condition) => LogicalFunction(left, Or, condition)
      case _               => left
    }

    def or(right: TableColumn[Boolean]): TableColumn[Boolean] = LogicalFunction(left, Or, right)

    def xor(right: Option[TableColumn[Boolean]]): TableColumn[Boolean] = right match {
      case Some(condition) => LogicalFunction(left, Xor, condition)
      case _               => left
    }

    def xor(right: TableColumn[Boolean]): TableColumn[Boolean] = LogicalFunction(left, Xor, right)
  }

  implicit class OptionalTableColumnImpr(left: Option[TableColumn[Boolean]]) {

    def and(right: Option[TableColumn[Boolean]]): Option[TableColumn[Boolean]] = (left, right) match {
      case (Some(l), Some(r)) => Option(LogicalFunction(l, And, r))
      case (None, Some(r))    => Some(r)
      case (Some(l), None)    => Some(l)
      case (None, None)       => None
    }

    def and(right: TableColumn[Boolean]): TableColumn[Boolean] = left match {
      case Some(l) => LogicalFunction(l, And, right)
      case _       => right
    }

    def or(right: Option[TableColumn[Boolean]]): Option[TableColumn[Boolean]] = (left, right) match {
      case (Some(l), Some(r)) => Option(LogicalFunction(l, Or, r))
      case (None, Some(r))    => Some(r)
      case (Some(l), None)    => Some(l)
      case (None, None)       => None
    }

    def or(right: TableColumn[Boolean]): TableColumn[Boolean] = left match {
      case Some(l) => LogicalFunction(l, Or, right)
      case _       => right
    }

    def xor(right: Option[TableColumn[Boolean]]): Option[TableColumn[Boolean]] = (left, right) match {
      case (Some(l), Some(r)) => Option(LogicalFunction(l, Xor, r))
      case (None, Some(r))    => Some(r)
      case (Some(l), None)    => Some(l)
      case (None, None)       => None
    }

    def xor(right: TableColumn[Boolean]): TableColumn[Boolean] = left match {
      case Some(l) => LogicalFunction(l, Xor, right)
      case _       => right
    }
  }

  implicit class LogicalOpsMagnetImpr(left: LogicalOpsMagnet) {

    def and(right: LogicalOpsMagnet): Option[TableColumn[Boolean]] = (left.asOption, right.asOption) match {
      case (Some(l), Some(r)) => Option(LogicalFunction(l, And, r))
      case (None, Some(r))    => Some(r)
      case (Some(l), None)    => Some(l)
      case (None, None)       => None
    }

    def or(right: LogicalOpsMagnet): Option[TableColumn[Boolean]] = (left.asOption, right.asOption) match {
      case (Some(l), Some(r)) => Option(LogicalFunction(l, Or, r))
      case (None, Some(r))    => Some(r)
      case (Some(l), None)    => Some(l)
      case (None, None)       => None
    }

    def xor(right: LogicalOpsMagnet): Option[TableColumn[Boolean]] = (left.asOption, right.asOption) match {
      case (Some(l), Some(r)) => Option(LogicalFunction(l, Xor, r))
      case (None, Some(r))    => Some(r)
      case (Some(l), None)    => Some(l)
      case (None, None)       => None
    }
  }
}
