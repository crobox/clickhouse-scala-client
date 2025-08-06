package com.crobox.clickhouse.dsl

/**
 * QueryFactory exposes all methods of OperationalQuery from a empty starting point (factoring new queries)
 */
trait QueryFactory extends OperationalQuery {
  override val internalQuery: InternalQuery = InternalQuery()

  override def withExpression(name: String, expression: Column): OperationalQuery =
    WithQuery(Seq(WithExpression(name, expression, isSubquery = false)))

  override def withExpressions(expressions: (String, Column)*): OperationalQuery =
    WithQuery(expressions.map { case (name, expr) => WithExpression(name, expr, isSubquery = false) })
}
