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
    toSQL(select(arrayMap[Double, Int, Double]((price, qty) => price * qty, prices, quantities)), false) should
    matchSQL("SELECT arrayMap((x,y) -> x * y, prices, quantities)")
  }

  it should "map over three arrays of different element types" in {
    val expr = arrayMap[String, Double, Int, String](
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
      select(arrayMap((price: TableColumn[Double], qty: TableColumn[Int]) => price * qty, prices, quantities)),
      false
    ) should matchSQL("SELECT arrayMap((x,y) -> x * y, prices, quantities)")
  }

  // arrayFilter returns elements of the FIRST array, so filtering ids by a predicate over quantities yields ids.
  it should "filter one array by a predicate over another, keeping the first array's type" in {
    val filtered: ExpressionColumn[Iterable[String]] =
      arrayFilter[String, Int]((_, qty) => qty > 0, ids, quantities)
    toSQL(select(filtered), false) should matchSQL("SELECT arrayFilter((x,y) -> y > 0, ids, quantities)")
  }

  it should "sum a lambda's output over two differently typed arrays" in {
    val total: ExpressionColumn[Double] =
      arraySum[Double, Int, Double]((price, qty) => price * qty, prices, quantities)
    toSQL(select(total), false) should matchSQL("SELECT arraySum((x,y) -> x * y, prices, quantities)")
  }

  // The server returns the first array sorted by the lambda's value, not the lambda's values themselves.
  it should "type the sorting family by the first array rather than the lambda output" in {
    // The lambda has to yield an ExpressionColumn, so the sort key is an expression over the quantity rather than
    // the bare column.
    val sorted: ExpressionColumn[Iterable[String]] =
      arraySort[String, Int, Int]((_, qty) => negate(qty), ids, quantities)
    val reversed: ExpressionColumn[Iterable[String]] =
      arrayReverseSort[String, Int, Int]((_, qty) => negate(qty), ids, quantities)
    toSQL(select(sorted, reversed), false) should matchSQL(
      "SELECT arraySort((x,y) -> -y, ids, quantities), arrayReverseSort((x,y) -> -y, ids, quantities)"
    )
  }

  it should "carry four and five arrays under the same name" in {
    val four = arrayMap[String, Double, Int, Int, String](
      (id, price, qty, bonus) => concat(id, const(":"), toStringRep(price * qty + bonus)),
      ids,
      prices,
      quantities,
      quantities
    )
    toSQL(select(four), false) should matchSQL(
      "SELECT arrayMap((x,y,z,u) -> concat(x, ':', toString((y * z) + u)), ids, prices, quantities, quantities)"
    )

    val five = arrayFilter[String, Double, Int, Int, Int](
      (_, _, _, _, last) => last > const(0),
      ids,
      prices,
      quantities,
      quantities,
      quantities
    )
    toSQL(select(five), false) should matchSQL(
      "SELECT arrayFilter((x,y,z,u,v) -> v > 0, ids, prices, quantities, quantities, quantities)"
    )
  }

  // One name per family, resolved by argument count -- there is no arrayMap2/arrayMap3 any more.
  it should "resolve every arity through the one name" in {
    toSQL(select(arrayMap[Double, Double](p => p * const(2d), prices)), false) should
    matchSQL("SELECT arrayMap(x -> x * 2.0, prices)")
    toSQL(select(arrayMap[Double, Int, Double]((p, q) => p * q, prices, quantities)), false) should
    matchSQL("SELECT arrayMap((x,y) -> x * y, prices, quantities)")
  }

  it should "reject a lambda whose parameter type does not match its array" in {
    """arrayMap[String, Int, String]((id, qty) => id, quantities, quantities)""" shouldNot typeCheck
  }

  // arraySplit and arrayReverseSplit now follow the same naming as every other family: the bare name is arity 1.
  it should "split at every arity the server accepts" in {
    toSQL(select(arraySplit[Int](x => x > const(1), quantities)), false) should
    matchSQL("SELECT arraySplit(x -> x > 1, quantities)")
    toSQL(select(arraySplit[String, Int]((_, qty) => qty > const(0), ids, quantities)), false) should
    matchSQL("SELECT arraySplit((x,y) -> y > 0, ids, quantities)")
    toSQL(select(arrayReverseSplit[Int](x => x > const(1), quantities)), false) should
    matchSQL("SELECT arrayReverseSplit(x -> x > 1, quantities)")
  }

  // A conditional is an ExpressionColumn whether or not it has cases, so it can be a lambda body directly. It used to
  // be typed TableColumn, which no higher-order function accepts, and callers reached for a cast.
  it should "take a conditional as a lambda body" in {
    val expr = arrayMap[Double, Int, Double](
      (price, qty) => multiIf(const(0.0), columnCase(qty > const(0), price * qty)),
      prices,
      quantities
    )
    toSQL(select(expr), false) should matchSQL(
      "SELECT arrayMap((x,y) -> if(y > 0, x * y, 0.0), prices, quantities)"
    )
  }
}
