package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.ClickhouseStatement

/**
 * @author Sjoerd Mulder
 * @since 2-1-17
 */
case class CreateDatabase(dbName: String, ifNotExists: Boolean = false, clusterName : Option[String] = None) extends ClickhouseSchemaStatement with DistributedDdlSupport {

  require(ClickhouseStatement.isValidIdentifier(dbName), s"Invalid database name identifier")
  requireValidCluster("Cannot create a database with an invalid cluster name")
  /**
   * Returns the query string for this statement.
   *
   * @return String containing the Clickhouse dialect SQL statement
   */
  override def query: String = s"CREATE DATABASE${printIfNotExists(ifNotExists)} $dbName${printOnCluster()}"
}
