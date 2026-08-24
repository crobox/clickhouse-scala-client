package com.crobox.clickhouse.dsl

/**
 * How much of a table to read.
 *
 * `rate` is either a fraction in (0, 1] or a row count above 1, matching ClickHouse's own overloading of the argument.
 * Only meaningful on a table whose engine declares a `SAMPLE BY` expression -- the server rejects it otherwise with
 * SAMPLING_NOT_SUPPORTED.
 */
case class Sample(rate: Double, offset: Option[Double] = None)

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
