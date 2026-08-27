package com.crobox.clickhouse.dsl

/**
 * The lambda of a higher-order array function, with one case per arity.
 *
 * An ADT rather than one `Option` slot per arity on every function node: the arity then lives in one place, each case
 * names its own parameter types independently, and supporting one more arity is a case here plus a rendering arm rather
 * than a change to all eighteen nodes.
 *
 * Each array a lambda draws from may have its own element type. ClickHouse only requires the arrays to be of equal
 * *length* -- "several arrays of identical length" -- so binding them to one type, as this once did, made
 * `arrayMap((id, price, qty) -> ..., ids, prices, quantities)` unwritable with no cast able to unify a String with a
 * numeric.
 *
 * Lives here rather than beside the function nodes in `dsl.column` because it is a structure and not a function: it
 * renders nothing on its own, and the coverage gate over `dsl.column` rightly expects everything there to.
 */
sealed trait Lambda[O]

object Lambda {
  case class Of1[I1, O](f: TableColumn[I1] => ExpressionColumn[O])                        extends Lambda[O]
  case class Of2[I1, I2, O](f: (TableColumn[I1], TableColumn[I2]) => ExpressionColumn[O]) extends Lambda[O]
  case class Of3[I1, I2, I3, O](f: (TableColumn[I1], TableColumn[I2], TableColumn[I3]) => ExpressionColumn[O])
      extends Lambda[O]
  case class Of4[I1, I2, I3, I4, O](
      f: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4]) => ExpressionColumn[O]
  ) extends Lambda[O]
  case class Of5[I1, I2, I3, I4, I5, O](
      f: (TableColumn[I1], TableColumn[I2], TableColumn[I3], TableColumn[I4], TableColumn[I5]) => ExpressionColumn[O]
  ) extends Lambda[O]
}
