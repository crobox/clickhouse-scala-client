package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslTestSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType

/**
 * Multi-array higher-order functions over arrays whose element types differ.
 *
 * Previously every array in one call was bound to a single element type, so a lambda over an id array and a numeric
 * array could not be written at all -- and no cast unifies a String with a numeric, so the three-array case had no
 * workaround.
 */
class HeterogeneousHigherOrderTest extends DslTestSpec {

  private val ids        = NativeColumn[Seq[String]]("ids", ColumnType.Array(ColumnType.String))
  private val prices     = NativeColumn[Seq[Double]]("prices", ColumnType.Array(ColumnType.Float64))
  private val quantities = NativeColumn[Seq[Int]]("quantities", ColumnType.Array(ColumnType.UInt32))

  it should "map over two arrays of different element types" in {
    toSQL(select(arrayMap2[Double, Int, Double]((price, qty) => price * qty, prices, quantities)), false) should
    matchSQL("SELECT arrayMap((x,y) -> x * y, prices, quantities)")
  }

  it should "map over three arrays of different element types" in {
    val expr = arrayMap3[String, Double, Int, String](
      (id, price, qty) => concat(id, const(":"), toStringRep(price * qty)),
      ids,
      prices,
      quantities
    )
    toSQL(select(expr), false) should matchSQL(
      "SELECT arrayMap((x,y,z) -> concat(x, ':', toString(y * z)), ids, prices, quantities)"
    )
  }

  // The lambda comes before the arrays, so the element types cannot be inferred from them -- either pass the type
  // arguments or annotate the lambda's parameters. This predates the change; the pre-existing tests all spell the
  // type arguments out too.
  it should "accept annotated lambda parameters instead of explicit type arguments" in {
    toSQL(
      select(arrayMap2((price: TableColumn[Double], qty: TableColumn[Int]) => price * qty, prices, quantities)),
      false
    ) should matchSQL("SELECT arrayMap((x,y) -> x * y, prices, quantities)")
  }

  // arrayFilter returns elements of the FIRST array, so filtering ids by a predicate over quantities yields ids.
  it should "filter one array by a predicate over another, keeping the first array's type" in {
    val filtered: ExpressionColumn[Iterable[String]] =
      arrayFilter2[String, Int]((_, qty) => qty > 0, ids, quantities)
    toSQL(select(filtered), false) should matchSQL("SELECT arrayFilter((x,y) -> y > 0, ids, quantities)")
  }

  it should "sum a lambda's output over two differently typed arrays" in {
    val total: ExpressionColumn[Double] =
      arraySum2[Double, Int, Double]((price, qty) => price * qty, prices, quantities)
    toSQL(select(total), false) should matchSQL("SELECT arraySum((x,y) -> x * y, prices, quantities)")
  }

  // The server returns the first array sorted by the lambda's value, not the lambda's values themselves.
  it should "type the sorting family by the first array rather than the lambda output" in {
    // The lambda has to yield an ExpressionColumn, so the sort key is an expression over the quantity rather than
    // the bare column.
    val sorted: ExpressionColumn[Iterable[String]] =
      arraySort2[String, Int, Int]((_, qty) => negate(qty), ids, quantities)
    val reversed: ExpressionColumn[Iterable[String]] =
      arrayReverseSort2[String, Int, Int]((_, qty) => negate(qty), ids, quantities)
    toSQL(select(sorted, reversed), false) should matchSQL(
      "SELECT arraySort((x,y) -> -y, ids, quantities), arrayReverseSort((x,y) -> -y, ids, quantities)"
    )
  }

  it should "reject a lambda whose parameter type does not match its array" in {
    """arrayMap2[String, Int, String]((id, qty) => id, quantities, quantities)""" shouldNot typeCheck
  }

  // arraySplit and arrayReverseSplit now follow the same naming as every other family: the bare name is arity 1.
  it should "split at every arity the server accepts" in {
    toSQL(select(arraySplit[Int](x => x > const(1), quantities)), false) should
    matchSQL("SELECT arraySplit(x -> x > 1, quantities)")
    toSQL(select(arraySplit2[String, Int]((_, qty) => qty > const(0), ids, quantities)), false) should
    matchSQL("SELECT arraySplit((x,y) -> y > 0, ids, quantities)")
    toSQL(select(arrayReverseSplit[Int](x => x > const(1), quantities)), false) should
    matchSQL("SELECT arrayReverseSplit(x -> x > 1, quantities)")
  }
}
