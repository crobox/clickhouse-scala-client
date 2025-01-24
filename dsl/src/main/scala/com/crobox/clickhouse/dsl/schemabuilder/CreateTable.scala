package com.crobox.clickhouse.dsl.schemabuilder

import com.crobox.clickhouse.dsl.{ClickhouseStatement, Table}

/**
 * @author
 *   Sjoerd Mulder
 * @since 30-12-16
 */
case class CreateTable(table: Table, engine: Engine, ifNotExists: Boolean = false, clusterName: Option[String] = None)
    extends ClickhouseSchemaStatement
    with DistributedDdlSupport {

  require(table.columns.nonEmpty, "Cannot create a table without any columns")

  /**
   * Returns the query string for this statement.
   *
   * @return
   *   String containing the Clickhouse dialect SQL statement
   */
//  TODO migrate this to the tokenizer as well
  override def query: String =
    s"""CREATE TABLE${printIfNotExists(ifNotExists)} ${table.quoted}${printOnCluster()} (
       |  ${table.columns.map(_.query).mkString(",\n  ")}
       |) ENGINE = $engine""".stripMargin
}
