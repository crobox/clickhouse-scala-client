package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{DslIntegrationSpec, dsl => CHDsl}

class StringFunctionsTest extends DslIntegrationSpec {

  it should "succeed for ScalaStringFunctions" in {
    r(toStringRep(pi()).contains("159")) shouldBe "1"
  }

  it should "succeed for SplitMergeFunctions" in {
    val theSeqOfStr = Seq("hey", "ow", "wo", "di", "ya!")

    val someStr        = theSeqOfStr.mkString(",")
    val someSep        = ","
    val someStrAsArray = "['hey','ow','wo','di','ya!']"

    r(splitByChar(someSep, someStr)) shouldBe someStrAsArray
    r(splitByString(someSep, someStr)) shouldBe someStrAsArray
    r(arrayStringConcat(theSeqOfStr, someSep)) shouldBe someStr
    r(alphaTokens(someStr)) shouldBe someStrAsArray.filterNot(_.equals('!'))
  }

  it should "succeed for StringFunctions" in {
    val someStr = const("hello world")

    r(CHDsl.empty(someStr)) shouldBe "0"
    r(notEmpty(someStr)) shouldBe "1"
    r(CHDsl.length(someStr)) shouldBe "11"
    r(lengthUTF8(someStr)) shouldBe "11"
    r(lower(someStr)) shouldBe "hello world"
    r(upper(someStr)) shouldBe "HELLO WORLD"
    r(lowerUTF8(someStr)) shouldBe "hello world"
    r(upperUTF8(someStr)) shouldBe "HELLO WORLD"
    r(reverse(someStr)) shouldBe "dlrow olleh"
    r(reverseUTF8(someStr)) shouldBe "dlrow olleh"
    r(concat(someStr, "!")) shouldBe "hello world!"
    r(substring(someStr, 2, 3)) shouldBe "ell"
    r(substringUTF8(someStr, 2, 4)) shouldBe "ello"
    r(appendTrailingCharIfAbsent(someStr, "!")) shouldBe "hello world!"
    //    r(convertCharset(someStr,"UTF16","UTF8")) shouldBe "hello world"
  }

  it should "succeed for StringSearchFunctions" in {
    val someStr    = const("hello world")
    val someNeedle = "lo"
    val replace    = "io"
    r(position(someStr, someNeedle)) shouldBe "4"
    r(positionCaseInsensitive(someStr, someNeedle)) shouldBe "4"
    r(positionUTF8(someStr, someNeedle)) shouldBe "4"
    r(positionUTF8CaseInsensitive(someStr, someNeedle)) shouldBe "4"
    r(strMatch(someStr, someNeedle)) shouldBe "1"
    r(extract(someStr, someNeedle)) shouldBe "lo"
    r(extractAll(someStr, someNeedle)) shouldBe "['lo']"
    r(like(someStr, someNeedle)) shouldBe "0"
    r(notLike(someStr, someNeedle)) shouldBe "1"
    r(replaceOne(someStr, someNeedle, replace)) shouldBe "helio world"
    r(replaceAll(someStr, someNeedle, replace)) shouldBe "helio world"
    r(replaceRegexpOne(someStr, someNeedle, replace)) shouldBe "helio world"
    r(replaceRegexpAll(someStr, someNeedle, replace)) shouldBe "helio world"
  }

  it should "keep empty as empty" in {
    val query = select(All()).from(TwoTestTable).where(col1.empty())
    toSql(query.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.twoTestTable WHERE empty(column_1)"
    )
  }

  it should "keep notEmpty as notEmtpy" in {
    var query = select(All()).from(TwoTestTable).where(col1.notEmpty())
    toSql(query.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.twoTestTable WHERE notEmpty(column_1)"
    )

    query = select(All()).from(TwoTestTable).where(notEmpty(col1))
    toSql(query.internalQuery, None) should matchSQL(
      s"SELECT * FROM $database.twoTestTable WHERE notEmpty(column_1)"
    )
  }
}
