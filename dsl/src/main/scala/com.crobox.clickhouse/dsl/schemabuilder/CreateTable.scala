package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.{ClickhouseStatement, Table}

/**
 * @author Sjoerd Mulder
 * @since 30-12-16
 */
case class CreateTable(table: Table,
                       engine: Engine,
                       ifNotExists: Boolean = false,
                       databaseName: String = ClickhouseStatement.DefaultDatabase)
    extends ClickhouseSchemaStatement {

  require(ClickhouseStatement.isValidIdentifier(table.name), "Cannot create a table with invalid identifier")
  require(ClickhouseStatement.isValidIdentifier(databaseName), "Cannot create a table with invalid database identifier")
  require(table.columns.nonEmpty, "Cannot create a table without any columns")

  /**
   * Returns the query string for this statement.
   *
   * @return String containing the Clickhouse dialect SQL statement
   */
//  TODO migrate this to the tokenizer as well
  override def query: String =
    s"""CREATE TABLE${printIfNotExists(ifNotExists)} $databaseName.${table.name} (
       |  ${table.columns.map(_.query()).mkString(",\n  ")}
       |) ENGINE = $engine""".stripMargin
}
