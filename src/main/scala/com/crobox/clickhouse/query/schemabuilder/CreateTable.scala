package com.crobox.clickhouse.query.schemabuilder

import com.crobox.clickhouse.ClickhouseStatement

/**
 * @author Sjoerd Mulder
 * @since 30-12-16
 */
case class CreateTable(tableName: String,
                       columns: Seq[Column],
                       engine: Engine,
                       ifNotExists: Boolean = false,
                       databaseName: String = ClickhouseStatement.DefaultDatabase)
    extends ClickhouseSchemaStatement {

  require(ClickhouseStatement.isValidIdentifier(tableName), "Cannot create a table with invalid identifier")
  require(ClickhouseStatement.isValidIdentifier(databaseName),
          "Cannot create a table with invalid database identifier")
  require(columns.nonEmpty, "Cannot create a table without any columns")

  /**
   * Returns the query string for this statement.
   *
   * @return String containing the Clickhouse dialect SQL statement
   */
  override def query: String =
    s"""CREATE TABLE${printIfNotExists(ifNotExists)} $databaseName.$tableName (
       |  ${columns.mkString(",\n  ")}
       |) ENGINE = $engine""".stripMargin
}
