package com.crobox.clickhouse.dsl.misc

import java.security.SecureRandom

import scala.annotation.tailrec

object RandomStringGenerator {
  private lazy val numberGenerator: SecureRandom = new SecureRandom

  def random(length: Int = 6): String =
    padLeft(Integer.toString(numberGenerator.nextInt(Int.MaxValue - 1) + 1, 36), length)

  @tailrec
  private def padLeft(string: String, length: Int): String = {
    if (string.length >= length) {
      return string
    }
    padLeft("0" + string, length)
  }
}
