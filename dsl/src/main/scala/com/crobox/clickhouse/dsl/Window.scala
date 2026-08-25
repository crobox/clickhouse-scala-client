package com.crobox.clickhouse.dsl

/** One edge of a window frame. */
sealed trait FrameBound

object FrameBound {
  case object UnboundedPreceding     extends FrameBound
  case object CurrentRow             extends FrameBound
  case object UnboundedFollowing     extends FrameBound
  case class Preceding(offset: Long) extends FrameBound
  case class Following(offset: Long) extends FrameBound
}

sealed abstract class FrameMode(val keyword: String)

object FrameMode {

  /** Counts rows. */
  case object Rows extends FrameMode("ROWS")

  /** Counts values of the ordering expression, so peers share a frame. */
  case object Range extends FrameMode("RANGE")
}

/**
 * `ROWS`/`RANGE` frame. With no `end` it renders the single-bound form, which ClickHouse reads as running to the
 * current row.
 */
case class WindowFrame(mode: FrameMode, start: FrameBound, end: Option[FrameBound] = None)

/** The parenthesised part of `OVER (...)`, and the right-hand side of a `WINDOW name AS (...)` definition. */
case class WindowSpec(
    partitionBy: Seq[Column] = Seq.empty,
    orderBy: Seq[OrderingColumn] = Seq.empty,
    frame: Option[WindowFrame] = None
)

/** What follows `OVER`: either a spec written in place, or the name of one defined in the `WINDOW` clause. */
sealed trait WindowRef

object WindowRef {
  case class Inline(spec: WindowSpec) extends WindowRef
  case class Named(name: String)      extends WindowRef
}

/** A `WINDOW name AS (spec)` definition, so several columns can share one spec. */
case class NamedWindow(name: String, spec: WindowSpec)
