package com.crobox.clickhouse.dsl.language
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{dsl => CHDsl}


class NumericColFunctionTest extends ColumnFunctionTest {
  "Tokenisations of functions working with numbers" should "succeed for ArithmeticFunctions" in {
    r(plus(3,3)) shouldBe "6"
    r(minus(3,2)) shouldBe "1"
    r(multiply(3,5)) shouldBe "15"
    r(divide(3,2)) shouldBe "1.5"
    r(intDiv(5,3)) shouldBe "1"
    r(intDivOrZero(5,3)) shouldBe "1"
    r(modulo(13,4)) shouldBe "1"
    r(gcd(3,2)) shouldBe "1"
    r(lcm(3,2)) shouldBe "6"
    r(negate(3)) shouldBe "-3"
    r(abs(-3)) shouldBe "3"
  }

  it should "succeed for LogicalFunctions" in {
    val constTrue = Const(true)

    r(true and Some(true)) shouldBe "1"
    r(true and false) shouldBe "0"
    r(constTrue or false) shouldBe "1"
    r(true or false) shouldBe "1"
    r(false or false) shouldBe "0"
    r(true xor constTrue) shouldBe "0"
    r( true xor Some(false)) shouldBe "1"
    r( true xor None) shouldBe "1"
    r( true or None) shouldBe "1"
    r( true and None) shouldBe "1"
    r(CHDsl.not(true)) shouldBe "0"
  }

  it should "properly reduce any constant result of LogicalFunctions" in {
    toSql(
      select(
        CHDsl.not(true and None) and true or false xor None
      ).internalQuery,
      None
    ) shouldBe "SELECT 0"

    toSql(
      select(
        Some(false) or CHDsl.not(ref[Boolean]("mazaa") and None) and true or false xor None
      ).internalQuery,
      None
    ) shouldBe "SELECT not(mazaa)"
  }

  it should "succeed for MathFunctions" in {
    r(e()) should startWith("2.718281828")
    r(pi()) should startWith("3.14159")
    r(exp(divide[Int,Int,Int](123,1))) should startWith("2.619")
    r(log(123)) should startWith("4.812184")
    r(exp2(123)) should startWith("1.063382396627")
    r(log2(123)) should startWith("6.9425145")
    r(exp10(123)) should startWith("1e123")
    r(log10(123)) should startWith("2.0899")
    r(sqrt(123)) should startWith("11.090")
    r(cbrt(123)) should startWith("4.9731")
    r(erf(123)) shouldBe("1")
    r(erfc(123)) shouldBe("0")
    r(lgamma(123)) should startWith("467.41")
    r(tgamma(123)) should startWith("9.8750")
    r(sin(123)) should startWith("-0.45990")
    r(cos(123)) should startWith("-0.88796")
    r(tan(123)) should startWith("0.51792747")
    r(asin(1)) should startWith("1.5707")
    r(acos(1)) shouldBe("0")
    r(atan(1)) should startWith("0.78539")
    r(pow(123,2)) shouldBe("15129")
  }

  it should "succeed for RandomFunctions" in {
    r(rand()).length should be > 0
    r(rand64()).length should be > 0
  }

  it should "succeed for RoundingFunctions" in {
    val someNum = const(123.456)
    r(floor(someNum,2)) shouldBe "123.45"
    r(ceil(someNum,2)) shouldBe "123.46"
    r(round(someNum,2)) shouldBe "123.46"
    r(roundToExp2(someNum)) shouldBe "64"
    r(roundDuration(someNum)) shouldBe "120"
    r(roundAge(someNum)) shouldBe "55"
  }

  it should "chain expressions" in {
    r(abs(const(1) / 2 + 3 - 4)) shouldBe "0.5"
  }

  it should "succeed for BitFunctions" in {
    r(bitAnd(0,1)) shouldBe "0"
    r(bitOr(0,1)) shouldBe "1"
    r(bitXor(0,1)) shouldBe "1"
    r(bitNot(64)) shouldBe (255 - 64).toString
    r(bitShiftLeft(2,2)) shouldBe "8"
    r(bitShiftRight(8,1)) shouldBe "4"
  }
}
