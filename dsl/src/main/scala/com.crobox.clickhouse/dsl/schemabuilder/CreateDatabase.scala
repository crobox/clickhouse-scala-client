package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.ClickhouseStatement

/**
 * @author Sjoerd Mulder
 * @since 2-1-17
 */
case class CreateDatabase(dbName: String, ifNotExists: Boolean = false) extends ClickhouseSchemaStatement {

  require(ClickhouseStatement.isValidIdentifier(dbName), s"Invalid database name identifier")

  /**
   * Returns the query string for this statement.
   *
   * @return String containing the Clickhouse dialect SQL statement
   */
  override def query: String = s"CREATE DATABASE${printIfNotExists(ifNotExists)} $dbName"
}
