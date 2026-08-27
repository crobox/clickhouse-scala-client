package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._

/**
 * Table functions, which stand where a table would in `FROM`.
 *
 * [[tableFunction]] reaches any of them, including the ones with no wrapper here and any the server gains later; the
 * named builders below exist for the common ones. Their arguments are [[Column]]s, so `const` renders a literal and
 * `tuple` renders a row.
 *
 * https://clickhouse.com/docs/sql-reference/table-functions
 */
trait TableFunctions {

  /** Any table function, by name. The escape hatch, and what every builder below is built on. */
  def tableFunction(name: String, args: Column*): TableFunctionFromQuery =
    TableFunctionFromQuery(TableFunction(name, args))

  /** `numbers(count)`, a single UInt64 column counting up from zero. */
  def numbers(count: Long): TableFunctionFromQuery = tableFunction("numbers", const(count))

  /** `numbers(start, count)`. */
  def numbers(start: Long, count: Long): TableFunctionFromQuery =
    tableFunction("numbers", const(start), const(count))

  /** `zeros(count)`, which is `numbers` without the counter -- cheaper when only the row count matters. */
  def zeros(count: Long): TableFunctionFromQuery = tableFunction("zeros", const(count))

  /** `values(rows)`, an inline table. Build each row with `tuple(...)`; a single-column table can take bare values. */
  def values(row: Column, rows: Column*): TableFunctionFromQuery = tableFunction("values", (row +: rows): _*)

  /** `values('structure', rows)`, naming and typing the columns rather than leaving them as `c1`, `c2`. */
  def valuesWithStructure(structure: String, row: Column, rows: Column*): TableFunctionFromQuery =
    tableFunction("values", (const(structure) +: row +: rows): _*)

  /**
   * `merge(database, tablesRegexp)`, reading every table in `database` whose name matches.
   *
   * Named `mergeTables` rather than `merge`: the `-Merge` aggregate combinator already holds that name in the same
   * import, and an overload between a combinator and a table function would resolve by argument type rather than by
   * intent.
   */
  def mergeTables(database: String, tablesRegexp: String): TableFunctionFromQuery =
    tableFunction("merge", const(database), const(tablesRegexp))

  /** `remote(addresses, database, table)`. */
  def remote(addresses: String, database: String, table: String): TableFunctionFromQuery =
    tableFunction("remote", const(addresses), const(database), const(table))

  def remote(
      addresses: String,
      database: String,
      table: String,
      user: String,
      password: String
  ): TableFunctionFromQuery =
    tableFunction("remote", const(addresses), const(database), const(table), const(user), const(password))

  /** `cluster(name, database, table)`, which reads a cluster from the server's own configuration. */
  def cluster(name: String, database: String, table: String): TableFunctionFromQuery =
    tableFunction("cluster", const(name), const(database), const(table))

  /** `clusterAllReplicas(name, database, table)`, which unlike [[cluster]] reads every replica rather than one. */
  def clusterAllReplicas(name: String, database: String, table: String): TableFunctionFromQuery =
    tableFunction("clusterAllReplicas", const(name), const(database), const(table))

  /** `generateRandom('structure')`, rows of random values matching the structure. */
  def generateRandom(structure: String): TableFunctionFromQuery =
    tableFunction("generateRandom", const(structure))

  /** `generateRandom('structure', seed, maxStringLength, maxArrayLength)`, for a reproducible sequence. */
  def generateRandom(
      structure: String,
      randomSeed: Long,
      maxStringLength: Long,
      maxArrayLength: Long
  ): TableFunctionFromQuery =
    tableFunction(
      "generateRandom",
      const(structure),
      const(randomSeed),
      const(maxStringLength),
      const(maxArrayLength)
    )
}
