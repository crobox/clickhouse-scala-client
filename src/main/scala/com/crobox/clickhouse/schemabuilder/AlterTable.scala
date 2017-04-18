package com.crobox.clickhouse.schemabuilder

import com.crobox.clickhouse.ClickhouseStatement

/**
  * @author Sjoerd Mulder
  * @since 2-1-17
  */
case class AlterTable(tableName: String, actions: Seq[ColumnOperation]) extends ClickhouseSchemaStatement {

  require(ClickhouseStatement.isValidIdentifier(tableName), s"Invalid table name identifier $tableName")
  /**
    * Returns the query string for this statement.
    *
    * @return String containing the Clickhouse dialect SQL statement
    */
  override def query: String = s"ALTER TABLE $tableName ${actions.mkString(", ")}"

}

sealed trait ColumnOperation

object ColumnOperation {

  case class AddColumn(column: Column, after: Option[String] = None) extends ColumnOperation {
    private val afterString = after.map(" AFTER " + _).getOrElse("")

    override def toString: String = s"ADD COLUMN $column$afterString"
  }

  case class DropColumn(name: String) extends ColumnOperation {
    override def toString: String = s"DROP COLUMN $name"
  }

  case class ModifyColumn(column: Column) extends ColumnOperation {
    override def toString: String = s"MODIFY COLUMN $column"
  }

}