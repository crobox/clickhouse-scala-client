package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.ClickhouseStatement

/**
 * @author Sjoerd Mulder
 * @since 2-1-17
 */
case class CreateDatabase(dbName: String, ifNotExists: Boolean = false, clusterName : Option[String] = None)
  extends ClickhouseSchemaStatement with DistributedDdlSupport {

  /**
   * Returns the query string for this statement.
   *
   * @return String containing the Clickhouse dialect SQL statement
   */
  override def query: String = s"CREATE DATABASE${printIfNotExists(ifNotExists)} ${ClickhouseStatement.quoteIdentifier(dbName)}${printOnCluster()}"
}
