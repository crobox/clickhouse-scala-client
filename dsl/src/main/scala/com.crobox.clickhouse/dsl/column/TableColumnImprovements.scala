package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.TableColumn

object TableColumnImprovements {
  implicit class EnhancedTableColumn(source: TableColumn[Boolean]) {

    def and(option: Option[TableColumn[Boolean]]): TableColumn[Boolean] = option match {
      case Some(condition) => source and condition
      case _               => source
    }

    def and(condition: TableColumn[Boolean]): TableColumn[Boolean] = source and condition
  }

  implicit class EnhancedOptionTableColumn(source: Option[TableColumn[Boolean]]) {

    def and(condition: Option[TableColumn[Boolean]]): Option[TableColumn[Boolean]] = (source, condition) match {
      case (Some(s), Some(c)) => Option(s and c)
      case (None, Some(c))    => Some(c)
      case (Some(s), None)    => Some(s)
      case (None, None)       => None
    }

    def and(condition: TableColumn[Boolean]): TableColumn[Boolean] = source match {
      case Some(condition) => source and condition
      case _               => condition
    }
  }
}
