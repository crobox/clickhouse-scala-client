package com.crobox.clickhouse.dsl.misc

object StringUtils {

  /**
   * Only remove brackets if surrounded and no other 'intermediate' brackets are available
   */
  def removeSurroundingBrackets(value: String): String =
    if (value.startsWith("(") && value.endsWith(")") && value.count(_ == '(') == 1 && value.count(_ == ')') == 1) {
      value.substring(1, value.length - 1)
    } else value
}
