package com.crobox.clickhouse

import com.google.common.escape.Escapers

/**
  * @author Sjoerd Mulder
  * @since 2-1-17
  */

object ClickhouseStatement {
  val DefaultDatabase: String = "default"
  private val UnquotedIdentifier = "^[a-zA-Z_][0-9a-zA-Z_]*$"
  private val Escaper = Escapers.builder
    .addEscape('\\', "\\\\")
    .addEscape('\n', "\\n")
    .addEscape('\t', "\\t")
    .addEscape('\b', "\\b")
    .addEscape('\f', "\\f")
    .addEscape('\r', "\\r")
    .addEscape('\u0000', "\\0")
    .addEscape('\'', "\\'")
    .addEscape('`', "\\`")
    .build


  def isValidIdentifier(identifier: String): Boolean = identifier != null && identifier.matches(UnquotedIdentifier)

  def escape(input: String): String = {
    if (input == null) return "NULL"
    Escaper.escape(input)
  }

  def quoteIdentifier(input: String): String = {
    require(input != null, "Can't quote null as identifier")
    "`" + Escaper.escape(input) + "`"
  }
}

trait ClickhouseStatement {

  /**
    * Returns the query string for this statement.
    *
    * @return String containing the Clickhouse dialect SQL statement
    */
  def query: String

}
