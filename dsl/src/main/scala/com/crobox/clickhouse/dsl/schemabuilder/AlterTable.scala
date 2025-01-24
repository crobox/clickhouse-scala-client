package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.{ClickhouseStatement, NativeColumn}

/**
 * @author
 *   Sjoerd Mulder
 * @since 2-1-17
 */
case class AlterTable(tableName: String, actions: Seq[ColumnOperation]) extends ClickhouseSchemaStatement {

  /**
   * Returns the query string for this statement.
   *
   * @return
   *   String containing the Clickhouse dialect SQL statement
   */
  override def query: String =
    s"ALTER TABLE ${ClickhouseStatement.quoteIdentifier(tableName)} ${actions.mkString(", ")}"

}

sealed trait ColumnOperation

object ColumnOperation {

  case class AddColumn(column: NativeColumn[_], after: Option[String] = None) extends ColumnOperation {
    private val afterString = after.map(" AFTER " + _).getOrElse("")

    override def toString: String = s"ADD COLUMN ${column.query}$afterString"
  }

  case class DropColumn(name: String) extends ColumnOperation {

    override def toString: String = s"DROP COLUMN ${ClickhouseStatement.quoteIdentifier(name)}"
  }

  case class ModifyColumn(column: NativeColumn[_]) extends ColumnOperation {

    override def toString: String = s"MODIFY COLUMN ${column.query}"
  }

}
