package com.crobox.clickhouse.testkit

import org.scalatest.matchers.{MatchResult, Matcher}

trait ClickhouseMatchers {

  private def clean(value: String): String =
    value
      .replaceAll("\\s+", " ")
      .replace(" ( ", " (")
      .replace(" )", ")")
      .trim

  def matchSQL(expected: String): Matcher[String] = new Matcher[String] {

    def apply(left: String): MatchResult =
      MatchResult(clean(left) == clean(expected),
                  s"SQL messages don't match. \nInput:     ${clean(left)}\n!= \nExpected:  ${clean(expected)}",
                  "SQL messages are equal")
  }
}
