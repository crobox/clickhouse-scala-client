package com.crobox.clickhouse.dsl

import java.util.regex.Pattern

/**
 * @author
 *   Sjoerd Mulder
 * @since 2-1-17
 */
object ClickhouseStatement {
  val DefaultDatabase: String = "default"

  /**
   * Compiled once rather than per call: quoteIdentifier runs for every identifier of every query, and `String.matches`
   * recompiles its pattern on every invocation.
   */
  private val UnquotedIdentifier = Pattern.compile("^[a-zA-Z_][0-9a-zA-Z_]*$")

  /**
   * The Clickhouse escape sequence for `c`, or `null` when `c` needs no escaping.
   *
   * A match on Char compiles to a jump table, so this replaces Guava's `Escapers` -- a whole dependency, plus its five
   * transitive annotation artifacts, for this one nine-entry map.
   */
  private def escapeSequence(c: Char): String = c match {
    case '\\'     => "\\\\"
    case '\n'     => "\\n"
    case '\t'     => "\\t"
    case '\b'     => "\\b"
    case '\f'     => "\\f"
    case '\r'     => "\\r"
    case '\u0000' => "\\0"
    case '\''     => "\\'"
    case '`'      => "\\`"
    case _        => null
  }

  def escape(input: String): String = {
    if (input == null) return "NULL"

    // Fast path: most values contain nothing to escape, so scan first and hand back the original untouched.
    var idx = 0
    while (idx < input.length && escapeSequence(input.charAt(idx)) == null)
      idx += 1
    if (idx == input.length) return input

    // java.lang.StringBuilder, not scala's: scala's has no append(CharSequence, Int, Int), so the three arguments get
    // auto-tupled into append(Any) and a Tuple3 lands in the SQL.
    val builder = new java.lang.StringBuilder(input.length + 16)
    builder.append(input, 0, idx)
    while (idx < input.length) {
      val escaped = escapeSequence(input.charAt(idx))
      if (escaped == null) builder.append(input.charAt(idx)) else builder.append(escaped)
      idx += 1
    }
    builder.toString
  }

  def quoteIdentifier(input: String): String = {
    require(input != null, "Can't quote null as identifier")
    require(input != "", "Can't quote empty string as identifier")
    if (UnquotedIdentifier.matcher(input).matches()) {
      input
    } else {
      "`" + escape(input) + "`"
    }
  }
}

trait ClickhouseStatement {

  /**
   * Returns the query string for this statement.
   *
   * @return
   *   String containing the Clickhouse dialect SQL statement
   */
  def query: String

}
