package com.crobox.clickhouse.dsl

/**
 * One entry in a `WITH` clause.
 *
 * ClickHouse puts three different things behind the same keyword, and the first two read in opposite orders:
 *   - [[WithExpression]] -- `WITH <expression> AS <name>`, naming a plain expression
 *   - [[WithScalarQuery]] -- `WITH (<subquery>) AS <name>`, naming the single value a subquery returns
 *   - [[WithTable]] -- `WITH <name> AS (<subquery>)`, a CTE you can select FROM
 *
 * A name introduced here is referenced with `ref`, which already exists for exactly this: `ref[Long]("total")`.
 *
 * Entries are scoped to the `SELECT` that declares them, so a subquery or a `UNION ALL` branch carries its own.
 */
sealed trait WithEntry {
  def name: String
}

/** `WITH <expression> AS <name>`, as in `WITH 1 AS one`. */
case class WithExpression(expression: Column, name: String) extends WithEntry

/**
 * `WITH (<subquery>) AS <name>`, where the subquery returns a single value -- the form requested in #142:
 * {{{WITH (SELECT sum(bytes) FROM system.parts WHERE active) AS total_disk_usage}}}
 */
case class WithScalarQuery(query: OperationalQuery, name: String) extends WithEntry

/**
 * `WITH <name> AS (<subquery>)`, a CTE.
 *
 * Also a [[Table]], so the same value both declares the CTE and selects from it:
 * {{{
 * val recent = WithTable("recent", select(itemId).from(TwoTestTable))
 * select(itemId).from(recent).withCte(recent)
 * }}}
 *
 * `columns` is empty because a CTE's shape is whatever its query projects rather than a declared schema; name its
 * columns with `ref` or reuse the columns of the tables it selects from.
 */
case class WithTable(name: String, query: OperationalQuery) extends WithEntry with Table {

  /** A CTE is referenced by a bare identifier -- there is no database to qualify it with. */
  override def database: String = ""

  override lazy val quoted: String = ClickhouseStatement.quoteIdentifier(name)

  override val columns: Seq[NativeColumn[_]] = Seq.empty
}
