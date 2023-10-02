package com.crobox.clickhouse.dsl.misc

import scala.util.Random

object RandomStringGenerator {

  /**
   * Generates a random string that can be used o.a. for alias names in Joins.
   * 'Uniqueness' is defined by the length of the generated sequence, which is default set to 6
   *
   * @param length
   * @return
   */
  def random(length: Int = 6): String = {
    // Alphanumeric includes numbers which can be used as first character in ClickHouse
    //Random.alphanumeric.take(length).mkString("")
    Random.nextString(length)
  }
}
