package com.crobox.clickhouse.dsl

object TableColumnImprovements {
  implicit class EnhancedTableColumn(source: TableColumn[Boolean]) {

    def and(option: Option[TableColumn[Boolean]]): TableColumn[Boolean] = option match {
      case Some(condition) => LogicalFunction(source, And, condition)
      case _               => source
    }

    def and(condition: TableColumn[Boolean]): TableColumn[Boolean] = LogicalFunction(source, And, condition)

    def or(option: Option[TableColumn[Boolean]]): TableColumn[Boolean] = option match {
      case Some(condition) => LogicalFunction(source, Or, condition)
      case _               => source
    }

    def or(condition: TableColumn[Boolean]): TableColumn[Boolean] = LogicalFunction(source, Or, condition)

    def xor(option: Option[TableColumn[Boolean]]): TableColumn[Boolean] = option match {
      case Some(condition) => LogicalFunction(source, Xor, condition)
      case _               => source
    }

    def xor(condition: TableColumn[Boolean]): TableColumn[Boolean] = LogicalFunction(source, Xor, condition)
  }

  implicit class EnhancedOptionTableColumn(source: Option[TableColumn[Boolean]]) {

    def and(condition: Option[TableColumn[Boolean]]): Option[TableColumn[Boolean]] = (source, condition) match {
      case (Some(s), Some(c)) => Option(LogicalFunction(s, And, c))
      case (None, Some(c))    => Some(c)
      case (Some(s), None)    => Some(s)
      case (None, None)       => None
    }

    def and(condition: TableColumn[Boolean]): TableColumn[Boolean] = source match {
      case Some(s) => LogicalFunction(s, And, condition)
      case _       => condition
    }

    def or(condition: Option[TableColumn[Boolean]]): Option[TableColumn[Boolean]] = (source, condition) match {
      case (Some(s), Some(c)) => Option(LogicalFunction(s, Or, c))
      case (None, Some(c))    => Some(c)
      case (Some(s), None)    => Some(s)
      case (None, None)       => None
    }

    def or(condition: TableColumn[Boolean]): TableColumn[Boolean] = source match {
      case Some(s) => LogicalFunction(s, Or, condition)
      case _       => condition
    }

    def xor(condition: Option[TableColumn[Boolean]]): Option[TableColumn[Boolean]] = (source, condition) match {
      case (Some(s), Some(c)) => Option(LogicalFunction(s, Xor, c))
      case (None, Some(c))    => Some(c)
      case (Some(s), None)    => Some(s)
      case (None, None)       => None
    }

    def xor(condition: TableColumn[Boolean]): TableColumn[Boolean] = source match {
      case Some(s) => LogicalFunction(s, Xor, condition)
      case _       => condition
    }
  }
}
