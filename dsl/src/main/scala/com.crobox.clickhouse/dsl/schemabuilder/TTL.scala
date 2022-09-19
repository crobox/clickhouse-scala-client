package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.Column

/**
 * https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-ttl
 */
case class TTL(column: Column, expression: String) {
  override def toString: String = s"${column.name} + INTERVAL $expression"
}

object TTL {

  def ttl(expressions: Iterable[TTL]): Option[String] =
    if (expressions.nonEmpty) Option("TTL " + expressions.mkString(", ")) else None
}
