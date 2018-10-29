package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{EmptyColumn, ExpressionColumn}

trait RandomFunctions { self: Magnets =>

  abstract class RandomFunction() extends ExpressionColumn[Long](EmptyColumn())

  case class Rand() extends RandomFunction
  case class Rand64() extends RandomFunction

  def rand() = Rand()
  def rand64() = Rand64()
/*
rand
rand64
 */
}
