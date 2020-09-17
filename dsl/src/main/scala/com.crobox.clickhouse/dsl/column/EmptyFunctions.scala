package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Column, ExpressionColumn}

trait EmptyFunctions { self: Magnets =>

  sealed abstract class EmptyFunction[+V](val innerCol: Column) extends ExpressionColumn[V](innerCol)

  case class Empty(col: EmptyNonEmptyCol[_])    extends EmptyFunction[Boolean](col.column)
  case class NotEmpty(col: EmptyNonEmptyCol[_]) extends EmptyFunction[Boolean](col.column)

  trait EmptyOps[C] { self: EmptyNonEmptyCol[_] =>

    def empty(): Empty       = Empty(self)
    def notEmpty(): NotEmpty = NotEmpty(self)
  }

  def empty(col: EmptyNonEmptyCol[_]): Empty       = Empty(col: EmptyNonEmptyCol[_])
  def notEmpty(col: EmptyNonEmptyCol[_]): NotEmpty = NotEmpty(col: EmptyNonEmptyCol[_])
}
