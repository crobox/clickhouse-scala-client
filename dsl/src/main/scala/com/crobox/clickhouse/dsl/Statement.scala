package com.crobox.clickhouse.dsl

/**
 * A statement that changes data, as opposed to a query that reads it.
 *
 * Deliberately outside [[InternalQuery]], for the reason [[ExplainKind]] is: these wrap a whole statement rather than
 * forming a clause inside one. As a field, `toRawSql` would render them inside a subquery's parentheses and a query
 * merge would propagate them.
 *
 * The `WHERE` is an ordinary [[TableColumn]], so it is built with the same DSL as a `SELECT`'s and rendered by the same
 * tokenizer -- which is the point: the alternative is composing the condition with the DSL and splicing the rendered
 * string into hand-written SQL, where nothing checks the escaping.
 *
 * `ON CLUSTER` requires ZooKeeper (or Keeper) to be configured; without it the server answers NO_ELEMENTS_IN_CONFIG at
 * execution time rather than at parse time.
 */
sealed trait Statement {
  def table: Table
  def onCluster: Option[String]
}

/**
 * `DELETE FROM table WHERE condition` -- a lightweight delete, which marks rows deleted rather than rewriting the parts
 * that hold them. [[AlterDelete]] is the mutation, and the two are not interchangeable operationally.
 */
case class DeleteFrom(
    table: Table,
    where: TableColumn[Boolean],
    onCluster: Option[String] = None
) extends Statement

/**
 * `ALTER TABLE table DELETE WHERE condition` -- a mutation, which rewrites every part the condition touches and runs
 * asynchronously in the background. Progress shows up in `system.mutations`.
 */
case class AlterDelete(
    table: Table,
    where: TableColumn[Boolean],
    onCluster: Option[String] = None
) extends Statement

/** One `column = expression` of an [[AlterUpdate]]. */
case class Assignment(column: Column, value: Column)

/**
 * `ALTER TABLE table UPDATE assignments WHERE condition`, a mutation.
 *
 * The mutation spelling rather than the standalone `UPDATE table SET ...`: that one does not exist in 25.3, the oldest
 * release this library supports, where it fails as a syntax error. This form works across the whole supported range.
 *
 * A column in the table's sorting key cannot be assigned to; the server answers CANNOT_UPDATE_COLUMN.
 */
case class AlterUpdate(
    table: Table,
    assignments: Seq[Assignment],
    where: TableColumn[Boolean],
    onCluster: Option[String] = None
) extends Statement {
  require(assignments.nonEmpty, "UPDATE needs at least one assignment")
}

/** Which partition [[DropPartition]] drops. */
sealed trait PartitionRef

object PartitionRef {

  /** `PARTITION ID 'id'`, the literal part name as `system.parts` reports it. */
  case class Id(id: String) extends PartitionRef

  /** `PARTITION expr`, the partitioning expression's value -- what `PARTITION BY` computes. */
  case class Expression(expr: Column) extends PartitionRef
}

/** `ALTER TABLE table DROP PARTITION ...`, which discards the parts wholesale rather than rewriting them. */
case class DropPartition(
    table: Table,
    partition: PartitionRef,
    onCluster: Option[String] = None
) extends Statement
