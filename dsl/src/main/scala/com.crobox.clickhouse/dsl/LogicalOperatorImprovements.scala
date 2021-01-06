package com.crobox.clickhouse.dsl

object LogicalOperatorImprovements {

  implicit class LogicalOperatorImpr(source: LogicalOpsMagnet) {

    def and(option: Option[LogicalOpsMagnet]): LogicalOpsMagnet = option match {
      case Some(condition) => LogicalFunction(source, And, condition)
      case _               => source
    }

    def and(condition: LogicalOpsMagnet): LogicalOpsMagnet = LogicalFunction(source, And, condition)

    def or(option: Option[LogicalOpsMagnet]): LogicalOpsMagnet = option match {
      case Some(condition) => LogicalFunction(source, Or, condition)
      case _               => source
    }

    def or(condition: LogicalOpsMagnet): LogicalOpsMagnet = LogicalFunction(source, Or, condition)

    def xor(option: Option[LogicalOpsMagnet]): LogicalOpsMagnet = option match {
      case Some(condition) => LogicalFunction(source, Xor, condition)
      case _               => source
    }

    def xor(condition: LogicalOpsMagnet): LogicalOpsMagnet = LogicalFunction(source, Xor, condition)
  }

  implicit class OptionalLogicalOperatorImpr(source: Option[LogicalOpsMagnet]) {

    def and(condition: Option[LogicalOpsMagnet]): Option[LogicalOpsMagnet] = (source, condition) match {
      case (Some(s), Some(c)) => Option(LogicalFunction(s, And, c))
      case (None, Some(c))    => Some(c)
      case (Some(s), None)    => Some(s)
      case (None, None)       => None
    }

    def and(condition: LogicalOpsMagnet): LogicalOpsMagnet = source match {
      case Some(s) => LogicalFunction(s, And, condition)
      case _       => condition
    }

    def or(condition: Option[LogicalOpsMagnet]): Option[LogicalOpsMagnet] = (source, condition) match {
      case (Some(s), Some(c)) => Option(LogicalFunction(s, Or, c))
      case (None, Some(c))    => Some(c)
      case (Some(s), None)    => Some(s)
      case (None, None)       => None
    }

    def or(condition: LogicalOpsMagnet): LogicalOpsMagnet = source match {
      case Some(s) => LogicalFunction(s, Or, condition)
      case _       => condition
    }

    def xor(condition: Option[LogicalOpsMagnet]): Option[LogicalOpsMagnet] = (source, condition) match {
      case (Some(s), Some(c)) => Option(LogicalFunction(s, Xor, c))
      case (None, Some(c))    => Some(c)
      case (Some(s), None)    => Some(s)
      case (None, None)       => None
    }

    def xor(condition: LogicalOpsMagnet): LogicalOpsMagnet = source match {
      case Some(s) => LogicalFunction(s, Xor, condition)
      case _       => condition
    }
  }
}
