package com.crobox.clickhouse.testkit

import org.scalatest.matchers.{MatchResult, Matcher}

trait ClickhouseMatchers {

  private def clean(value: String): String =
    value
      .replaceAll("\\s+", " ")
      .replace(" ( ", " (")
      .replace(" )", ")")
      .trim

  private def diff(s1: String, s2: String): String =
    s1.zip(s2).map(tuple => if (tuple._1 == tuple._2) '_' else tuple._1).mkString("")

  def matchSQL(expected: String): Matcher[String] = new Matcher[String] {

    def apply(left: String): MatchResult =
      MatchResult(
        clean(left) == clean(expected),
        s"SQL messages don't match. \nInput:     ${clean(left)}\n!= \nExpected:  ${clean(expected)}" +
          s"\nDIFF       ${diff(clean(left), clean(expected))}",
        "SQL messages are equal"
      )
  }
}
