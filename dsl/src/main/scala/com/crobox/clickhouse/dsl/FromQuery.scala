package com.crobox.clickhouse.dsl

/**
 * How much of a table to read.
 *
 * `rate` is either a fraction in (0, 1] or a row count above 1, matching ClickHouse's own overloading of the argument.
 * Only meaningful on a table whose engine declares a `SAMPLE BY` expression -- the server rejects it otherwise with
 * SAMPLING_NOT_SUPPORTED.
 */
case class Sample(rate: Double, offset: Option[Double] = None)

/**
 * A table function standing where a table would, as in `FROM numbers(10)`.
 *
 * Arguments are [[Column]]s so that the existing literal rendering applies -- `const("system")` becomes `'system'`. A
 * table function that takes a query rather than values, such as `view`, has no representation here; the DSL already
 * puts a subquery in `FROM` directly.
 */
case class TableFunction(name: String, args: Seq[Column] = Seq.empty)

sealed trait FromQuery extends Query with OperationalQuery {
  override val internalQuery: InternalQuery = InternalQuery(from = Some(this))
  val alias: Option[String]
  val finalized: Boolean
  val sampling: Option[Sample]
}

sealed case class InnerFromQuery(innerQuery: OperationalQuery, alias: Option[String] = None) extends FromQuery {

  /** Queries can never have 'final' clause: Illegal FINAL */
  override val finalized = false

  /**
   * Nor SAMPLE, which reads a table's sampling key. Named like `finalized`, so it cannot collide with
   * OperationalQuery's `sample(rate, offset)`.
   */
  override val sampling: Option[Sample] = None
}

sealed case class TableFromQuery[T <: Table](
    table: T,
    alias: Option[String] = None,
    finalized: Boolean = false,
    sampling: Option[Sample] = None
) extends FromQuery

/**
 * `FROM <function>(args)`.
 *
 * Neither FINAL nor SAMPLE applies: both read something the table's engine declares, and a function has no engine.
 */
sealed case class TableFunctionFromQuery(function: TableFunction, alias: Option[String] = None) extends FromQuery {
  override val finalized                = false
  override val sampling: Option[Sample] = None
}
