package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._

class HigherOrderFunctionsIT extends DslITSpec {
  val arr1 = Seq(1L, 2L, 3L)

  it should "HigherOrderFunctions: arrayAll" in {
    r(arrayAll[Long](_ <= 3, arr1)) shouldBe "1"
    r(arrayAll[Long](_.isEq(2L), arr1)) shouldBe "0"

    r(arrayAll[Int, Int]((x, y) => x < y, Seq(1, 2, 3), Seq(1, 2, 3))) shouldBe "0"
    r(arrayAll[Int, Int]((x, y) => x < y, Seq(1, 2, 3), Seq(4, 5, 6))) shouldBe "1"
  }

  it should "HigherOrderFunctions: arrayAvg" in {
    r(arrayAvg[Long, Long](None, arr1)) shouldBe "2"
    r(arrayAvg[Long, Double](Option(x => x * 333d), arr1)) shouldBe "666"

    r(arrayAvg[Int, Int, Int]((x, y) => x * y, Seq(1, 3, 3, 0), Seq(3, 2, 3, 4))) shouldBe "4.5"
  }

  it should "HigherOrderFunctions: arrayCount" in {
    r(arrayCount[Long](Some(_.isEq(2L)), arr1)) shouldBe "1"
    r(arrayCount[Long](None, arr1)) shouldBe "3"

    r(arrayCount[Int, Int]((x, y) => x.notEq(y), Seq(1, 3, 3, 0), Seq(3, 2, 3, 4))) shouldBe "3"
    r(arrayCount[Int, Int]((x, y) => x.isEq(y), Seq(1, 3, 3, 0), Seq(3, 2, 3, 4))) shouldBe "1"
  }

  it should "HigherOrderFunctions: arrayCumSum" in {
    r(arrayCumSum[Long, Long](Some(_ * 2L), arr1)) shouldBe "[2,6,12]"
    r(arrayCumSum[Long, Long](None, arr1)) shouldBe "[1,3,6]"

    r(arrayCumSum[Long, Long, Long]((x, y) => x * y, arr1, arr1)) shouldBe "[1,5,14]"
  }

  it should "HigherOrderFunctions: arrayExists" in {
    r(arrayExists[Long](_.isEq(2L), arr1)) shouldBe "1"
    r(arrayExists[Long](_.isEq(-1L), arr1)) shouldBe "0"

    r(arrayExists[Int, Int]((x, y) => x.isEq(y), Seq(1, 2, 3), Seq(4, 5, 6))) shouldBe "0"
    r(arrayExists[Int, Int]((x, y) => x.notEq(y), Seq(1, 2, 3), Seq(4, 5, 6))) shouldBe "1"
  }

  it should "HigherOrderFunctions: arrayFill" in {
    r(arrayFill[Long](_.isEq(2L), arr1)) shouldBe "[1,2,2]"
  }

  it should "HigherOrderFunctions: arrayFilter" in {
    r(arrayFilter[Long](_ <> 2L, arr1)) shouldBe "[1,3]"
    r(arrayFilter[Long](_ < 0L, arr1)) shouldBe "[]"

    r(arrayFilter[String](_.like("%World%"), Seq("Hello", "World"))) shouldBe "['World']"
    r(
      arrayFilter[String, String](
        (x, y) => x.concat(y).like("%World"),
        Seq("Hello", "World"),
        Seq("Sjoerd", "Leonard")
      )
    ) shouldBe "[]"
  }

  it should "HigherOrderFunctions: arrayFirst" in {
    r(arrayFirst[Long](modulo(_, 2L).isEq(0), arr1)) shouldBe "2"
    r(arrayFirst[Long](_ < 0, arr1)) shouldBe "0"

    r(arrayFirst[Int, Int]((x, y) => x > y, Seq(1, 2, 3), Seq(1, 2, 2))) shouldBe "3"
  }

  it should "HigherOrderFunctions: arrayFirstIndex" in {
    r(arrayFirstIndex[Long](modulo(_, 2L).isEq(0), arr1)) shouldBe "2"
    r(arrayFirstIndex[Long](_ < 0, arr1)) shouldBe "0"

    r(arrayFirstIndex[Int, Int]((x, y) => x > y, Seq(1, 2, 3), Seq(1, 2, 2))) shouldBe "3"
  }

  it should "HigherOrderFunctions: arrayMap" in {
    r(arrayMap[Long, Long](x => x * 2L, arr1)) shouldBe "[2,4,6]"
    r(arrayMap[Long, Long, Long]((x, y) => x * y, arr1, arr1)) shouldBe "[1,4,9]"
    r(arrayMap[Long, Long, Long, Long]((x, y, z) => x * y * z, arr1, arr1, arr1)) shouldBe "[1,8,27]"
  }

  it should "HigherOrderFunctions: arrayMax" in {
    r(arrayMax[Long, Long](None, arr1)) shouldBe "3"
    r(arrayMax[Long, Long](Option(x => x * -1L), arr1)) shouldBe "-1"

    r(arrayMax[Int, Int, Int]((x, y) => x ^ y, Seq(1, 2, 3), Seq(1, 2, 2))) shouldBe "9"
  }

  it should "HigherOrderFunctions: arrayMin" in {
    r(arrayMin[Long, Long](None, arr1)) shouldBe "1"
    r(arrayMin[Long, Long](Option(x => x * -1L), arr1)) shouldBe "-3"

    r(arrayMin[Int, Int, Int]((x, y) => x ^ y, Seq(1, 2, 3), Seq(1, 2, 2))) shouldBe "1"
  }

  it should "HigherOrderFunctions: arrayReverseFill" in {
    r(arrayReverseFill[Long](_.isEq(2L), arr1)) shouldBe "[2,2,3]"
  }

  it should "HigherOrderFunctions: arrayReverseSort" in {
    // [Long, Long], not [Long, Int]: ClickHouse's `modulo(Int64, Int32)` is Int64 (verified with toTypeName), and the
    // AritRetType table now agrees -- it used to narrow Long % Int to Int, which is what this line was written against.
    r(arrayReverseSort[Long, Long](Some(_ % 3), arr1)) shouldBe "[2,1,3]"
    r(arrayReverseSort[Long, Long](None, arr1)) shouldBe "[3,2,1]"

    r(arrayReverseSort[Int, Int, Int]((x, y) => x * y, Seq(1, 3, 3, 0), Seq(3, 2, 3, 4))) shouldBe "[3,3,1,0]"
  }

  it should "HigherOrderFunctions: arrayReverseSplit" in {
    r(
      arrayReverseSplit[Int, Int]((x, y) => y.notEq(0), Iterable(1, 2, 3, 4, 5), Iterable(1, 0, 0, 1, 0))
    ) shouldBe "[[1],[2,3,4],[5]]"
  }

  it should "HigherOrderFunctions: arraySort" in {
    r(arraySort[Long, Double](Some(_ % 3.0), arr1)) shouldBe "[3,1,2]"
    r(arraySort[Long, Double](None, arr1)) shouldBe "[1,2,3]"
    r(arraySort[Int, Int, Int]((x, y) => x * y, Seq(1, 3, 3, 0), Seq(3, 2, 3, 4))) shouldBe "[0,1,3,3]"
  }

  it should "HigherOrderFunctions: arraySplit" in {
    r(
      arraySplit[Int, Int]((x, y) => y.notEq(0), Iterable(1, 2, 3, 4, 5), Iterable(1, 0, 0, 1, 0))
    ) shouldBe "[[1,2,3],[4,5]]"
  }

  it should "HigherOrderFunctions: arraySum" in {
    r(arraySum[Long, Long](Some(_ * 2L), arr1)) shouldBe "12"
    r(arraySum[Long, Long](None, arr1)) shouldBe "6"

    r(arraySum[Int, Int, Int]((x, y) => x ^ y, Seq(1, 2, 3), Seq(1, 2, 2))) shouldBe "14"
  }

  // Arrays whose element types differ. The three-array case had no workaround before: no cast unifies a String with a
  // numeric, so any expression over an id array plus two numeric arrays was unwritable.
  it should "HigherOrderFunctions: heterogeneous element types" in {
    val ids        = Iterable("a", "b")
    val prices     = Iterable(1.5d, 2.5d)
    val quantities = Iterable(2, 3)

    r(arrayMap[Double, Int, Double]((price, qty) => price * qty, prices, quantities)) shouldBe "[3,7.5]"

    r(
      arrayMap[String, Double, Int, String](
        (id, price, qty) => concat(id, const(":"), toStringRep(price * qty)),
        ids,
        prices,
        quantities
      )
    ) shouldBe "['a:3','b:7.5']"

    // arrayFilter keeps elements of the first array, so this yields ids selected by a predicate over quantities.
    r(arrayFilter[String, Int]((_, qty) => qty > 2, ids, quantities)) shouldBe "['b']"

    r(arraySum[Double, Int, Double]((price, qty) => price * qty, prices, quantities)) shouldBe "10.5"
  }

  // The sorting family returns the first array reordered, not the lambda's values -- which is why it is typed by the
  // first array's element type.
  it should "HigherOrderFunctions: arraySort returns the first array" in {
    r(arraySort[String, Int, Int]((_, qty) => negate(qty), Iterable("a", "b"), Iterable(1, 9))) shouldBe "['b','a']"
    // Arity one, with the lambda's output type differing from the array's: the result is still the array's own
    // elements, which the previous Iterable[O] typing got wrong.
    r(arraySort[Int, Double](Option(x => x * 1.5d), Iterable(3, 1))) shouldBe "[1,3]"
  }

  it should "HigherOrderFunctions: four and five arrays" in {
    val ids  = Iterable("a", "b")
    val ones = Iterable(1, 1)
    r(
      arrayMap[String, Int, Int, Int, String](
        (id, a, b, c) => concat(id, toStringRep(a + b + c)),
        ids,
        ones,
        ones,
        ones
      )
    ) shouldBe "['a3','b3']"

    r(
      arrayFilter[String, Int, Int, Int, Int](
        (_, _, _, _, last) => last > 0,
        ids,
        ones,
        ones,
        ones,
        Iterable(0, 1)
      )
    ) shouldBe "['b']"
  }

  it should "HigherOrderFunctions: split at arity one and three" in {
    r(arraySplit[Int](x => x > const(1), Iterable(1, 2, 3))) shouldBe "[[1],[2],[3]]"
    r(arrayReverseSplit[Int](x => x > const(1), Iterable(1, 2, 3))) shouldBe "[[1,2],[3]]"
    r(
      arraySplit[Int, Int, Int]((_, _, c) => c > const(0), Iterable(1, 2), Iterable(1, 2), Iterable(1, 0))
    ) shouldBe "[[1,2]]"
  }
}
