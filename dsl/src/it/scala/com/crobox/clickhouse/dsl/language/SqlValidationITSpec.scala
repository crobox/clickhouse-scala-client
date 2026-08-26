package com.crobox.clickhouse.dsl.language

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.dsl.schemabuilder.ColumnType
import com.crobox.clickhouse.time.{MultiDuration, MultiInterval, TimeUnit}

/**
 * Asserts that the SQL this DSL emits is SQL ClickHouse actually accepts.
 *
 * The gap this closes: a tokenizer arm is a string template, so a wrong one still type-checks and still passes a
 * hand-written `should matchSQL("...")` assertion as long as the expectation was written to match the bug. The tuple
 * access operator emitted `x.2)` -- an unmatched parenthesis -- for years, because nothing ever rendered it and no
 * expectation existed to contradict. Sending the generated text to ClickHouse's own parser is the only check that
 * cannot be written to agree with a mistake.
 *
 * Two strengths of check, because they need different things from the server:
 *   - [[Semantic]] uses `EXPLAIN QUERY TREE`, which resolves function names, arity and argument types. It needs the new
 *     analyzer, so it is only used from 24.3 onwards and degrades to a syntax check on older servers.
 *   - [[Syntax]] uses `EXPLAIN AST`, which parses and nothing more. This is all that is available for constructs whose
 *     names cannot resolve in a bare test database -- the dictionary functions need a configured dictionary.
 *
 * Neither form executes the query, so constructs may reference tables and columns freely and cost nothing to check.
 */
class SqlValidationITSpec extends DslITSpec {

  sealed trait ValidationLevel

  /** Parse, resolve names, check arity and argument types. */
  case object Semantic extends ValidationLevel

  /** Parse only. */
  case object Syntax extends ValidationLevel

  /**
   * @param ast
   *   for entries in [[astConstructs]], the simple name of the AST case class this exercises -- the coverage test
   *   matches it against the declarations in `dsl/column`, so it must spell one of them exactly. For [[joinShapes]] it
   *   is just a description, since those cover a whole-query shape rather than one node.
   */
  case class Construct(ast: String, query: OperationalQuery, level: ValidationLevel = Semantic)

  private val stampInAmsterdam =
    java.time.ZonedDateTime.of(2020, 6, 1, 12, 0, 0, 0, java.time.ZoneId.of("Europe/Amsterdam"))

  private val oneDay = MultiInterval(
    java.time.ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, java.time.ZoneOffset.UTC),
    java.time.ZonedDateTime.of(2020, 1, 8, 0, 0, 0, 0, java.time.ZoneOffset.UTC),
    MultiDuration(1, TimeUnit.Day)
  )

  private def sem(ast: String, query: OperationalQuery) = Construct(ast, query, Semantic)
  private def syn(ast: String, query: OperationalQuery) = Construct(ast, query, Syntax)

  private val nullableConstructs = Seq(
    sem("IsNull", select(isNull(1))),
    sem("IsNullable", select(isNullable(1))),
    sem("IsNotNull", select(isNotNull(1))),
    sem("IsZeroOrNull", select(isZeroOrNull(1))),
    sem("AssumeNotNull", select(assumeNotNull(1))),
    sem("ToNullable", select(toNullable(1))),
    sem("IfNull", select(ifNull(1, 0))),
    sem("NullIf", select(nullIf(1, 0)))
  )

  private val jsonConstructs = Seq(
    sem("VisitParamHas", select(visitParamHas("""{"a":1}""", "a"))),
    sem("VisitParamExtractUInt", select(visitParamExtractUInt("""{"a":1}""", "a"))),
    sem("VisitParamExtractInt", select(visitParamExtractInt("""{"a":-1}""", "a"))),
    sem("VisitParamExtractFloat", select(visitParamExtractFloat("""{"a":1.5}""", "a"))),
    sem("VisitParamExtractBool", select(visitParamExtractBool("""{"a":true}""", "a"))),
    sem("VisitParamExtractRaw", select(visitParamExtractRaw("""{"a":{"b":1}}""", "a"))),
    sem("VisitParamExtractString", select(visitParamExtractString("""{"a":"b"}""", "a")))
  )

  private val encodingConstructs = Seq(
    sem("Hex", select(hex(255))),
    sem("Unhex", select(unhex("FF"))),
    sem("UUIDStringToNum", select(uUIDStringToNum("00000000-0000-0000-0000-000000000000"))),
    sem("UUIDNumToString", select(uUIDNumToString(uUIDStringToNum("00000000-0000-0000-0000-000000000000")))),
    sem("BitmaskToList", select(bitmaskToList(5))),
    sem("BitmaskToArray", select(bitmaskToArray(5)))
  )

  private val ipConstructs = Seq(
    sem("IPv4NumToString", select(iPv4NumToString(3232235521L))),
    sem("IPv4StringToNum", select(iPv4StringToNum("192.168.0.1"))),
    sem("IPv4NumToStringClassC", select(iPv4NumToStringClassC(3232235521L))),
    sem("IPv6NumToString", select(iPv6NumToString(iPv6StringToNum("::1")))),
    sem("IPv6StringToNum", select(iPv6StringToNum("::1")))
  )

  private val splitMergeConstructs = Seq(
    sem("SplitByChar", select(splitByChar(",", "a,b"))),
    sem("SplitByString", select(splitByString(", ", "a, b"))),
    sem("ArrayStringConcat", select(arrayStringConcat(splitByChar(",", "a,b"), ","))),
    sem("AlphaTokens", select(alphaTokens("a1b2")))
  )

  private val emptyConstructs = Seq(
    sem("Empty", select(com.crobox.clickhouse.dsl.empty(""))),
    sem("NotEmpty", select(com.crobox.clickhouse.dsl.notEmpty("x")))
  )

  // Shapes lifted from HigherOrderFunctionsIT, which already runs them. Registered here so the lambda-parameter
  // binding is checked by the analyzer too, not only by the value each returns.
  private val higherOrderConstructs = {
    val nums = Seq(1L, 2L, 3L)
    Seq(
      sem("ArrayAll", select(arrayAll[Long](_ <= 3, nums))),
      sem("ArrayAvg", select(arrayAvg[Long, Long](None, nums))),
      sem("ArrayCount", select(arrayCount[Long](Some(_.isEq(2L)), nums))),
      sem("ArrayCumSum", select(arrayCumSum[Long, Long](None, nums))),
      sem("ArrayExists", select(arrayExists[Long](_.isEq(2L), nums))),
      sem("ArrayFill", select(arrayFill[Long](_.isEq(2L), nums))),
      sem("ArrayFilter", select(arrayFilter[Long](_ <> 2L, nums))),
      sem("ArrayFirst", select(arrayFirst[Long](_ < 0, nums))),
      sem("ArrayFirstIndex", select(arrayFirstIndex[Long](_ < 0, nums))),
      sem("ArrayMap", select(arrayMap[Long, Long](x => x * 2L, nums))),
      sem("ArrayMax", select(arrayMax[Long, Long](None, nums))),
      sem("ArrayMin", select(arrayMin[Long, Long](None, nums))),
      sem("ArrayReverseFill", select(arrayReverseFill[Long](_.isEq(2L), nums))),
      sem("ArrayReverseSort", select(arrayReverseSort[Long, Long](None, nums))),
      sem("ArraySort", select(arraySort[Long, Double](None, nums))),
      sem("ArraySum", select(arraySum[Long, Long](None, nums))),
      sem("ArraySplit", select(arraySplit[Int]((x, y) => y.notEq(0), Iterable(1, 2, 3), Iterable(1, 0, 1)))),
      sem(
        "ArrayReverseSplit",
        select(arrayReverseSplit[Int]((x, y) => y.notEq(0), Iterable(1, 2, 3), Iterable(1, 0, 1)))
      )
    )
  }

  private val arrayConstructs = {
    val nums = Seq(1, 2, 3)
    Seq(
      sem("EmptyArrayUInt8", select(emptyArrayUInt8)),
      sem("EmptyArrayUInt16", select(emptyArrayUInt16)),
      sem("EmptyArrayUInt32", select(emptyArrayUInt32)),
      sem("EmptyArrayUInt64", select(emptyArrayUInt64)),
      sem("EmptyArrayInt8", select(emptyArrayInt8)),
      sem("EmptyArrayInt16", select(emptyArrayInt16)),
      sem("EmptyArrayInt32", select(emptyArrayInt32)),
      sem("EmptyArrayInt64", select(emptyArrayInt64)),
      sem("EmptyArrayFloat32", select(emptyArrayFloat32)),
      sem("EmptyArrayFloat64", select(emptyArrayFloat64)),
      sem("EmptyArrayDate", select(emptyArrayDate)),
      sem("EmptyArrayDateTime", select(emptyArrayDateTime)),
      sem("EmptyArrayString", select(emptyArrayString)),
      sem("EmptyArrayToSingle", select(emptyArrayToSingle(emptyArrayUInt8))),
      sem("Range", select(range(5))),
      sem("ArrayConcat", select(arrayConcat(nums, nums))),
      sem("ArrayElement", select(arrayElement(nums, 1))),
      sem("Has", select(has(nums, 2))),
      sem("HasAll", select(hasAll(nums, Seq(1, 2)))),
      sem("HasAny", select(hasAny(nums, Seq(1, 9)))),
      sem("IndexOf", select(indexOf(nums, 2))),
      sem("CountEqual", select(countEqual(nums, 2))),
      sem("ArrayEnumerate", select(arrayEnumerate(nums))),
      sem("ArrayEnumerateUniq", select(arrayEnumerateUniq(nums))),
      sem("ArrayPopBack", select(arrayPopBack(nums))),
      sem("ArrayPopFront", select(arrayPopFront(nums))),
      sem("ArrayPushBack", select(arrayPushBack(nums, 4))),
      sem("ArrayPushFront", select(arrayPushFront(nums, 0))),
      sem("ArrayResize", select(arrayResize(nums, 5, 0))),
      sem("ArraySlice", select(arraySlice(nums, 1, 2))),
      sem("ArrayUniq", select(arrayUniq(nums))),
      sem("ArrayJoin", select(arrayJoin(nums))),
      sem("ArrayDifference", select(arrayDifference(nums))),
      sem("ArrayDistinct", select(arrayDistinct(nums))),
      sem("ArrayIntersect", select(arrayIntersect(nums, Seq(2, 3)))),
      sem("ArrayReduce", select(arrayReduce("sum", nums))),
      sem("ArrayReverse", select(arrayReverse(nums))),
      sem("ArrayEmpty", select(arrayEmpty(nums))),
      sem("ArrayNotEmpty", select(arrayNotEmpty(nums))),
      sem("ArrayLength", select(arrayLength(nums))),
      // arrayOf rather than the DSL's `Array`, which shadows scala.Array here and reads like a native array call.
      sem("ArrayFlatten", select(arrayFlatten(arrayOf(arrayOf("1", "2"), arrayOf("3")))))
    )
  }

  private val bitConstructs = Seq(
    sem("BitAnd", select(bitAnd(3, 5))),
    sem("BitOr", select(bitOr(3, 5))),
    sem("BitXor", select(bitXor(3, 5))),
    sem("BitNot", select(bitNot(3))),
    sem("BitShiftLeft", select(bitShiftLeft(3, 2))),
    sem("BitShiftRight", select(bitShiftRight(3, 2)))
  )

  private val randomConstructs = Seq(
    sem("Rand", select(rand())),
    sem("Rand64", select(rand64()))
  )

  private val roundingConstructs = Seq(
    sem("Floor", select(floor(1.5, 1))),
    sem("Ceil", select(ceil(1.5, 1))),
    sem("Round", select(round(1.5, 1))),
    sem("RoundToExp2", select(roundToExp2(5))),
    sem("RoundDuration", select(roundDuration(5))),
    sem("RoundAge", select(roundAge(25)))
  )

  private val mathConstructs = Seq(
    sem("E", select(e())),
    sem("Pi", select(pi())),
    sem("Exp", select(exp(1))),
    sem("Log", select(log(1))),
    sem("Exp2", select(exp2(1))),
    sem("Log2", select(log2(1))),
    sem("Exp10", select(exp10(1))),
    sem("Log10", select(log10(1))),
    sem("Sqrt", select(sqrt(4))),
    sem("Cbrt", select(cbrt(8))),
    sem("Erf", select(erf(1))),
    sem("Erfc", select(erfc(1))),
    sem("Lgamma", select(lgamma(1))),
    sem("Tgamma", select(tgamma(1))),
    sem("Sin", select(sin(1))),
    sem("Cos", select(cos(1))),
    sem("Tan", select(tan(1))),
    sem("Asin", select(asin(0.5))),
    sem("Acos", select(acos(0.5))),
    sem("Atan", select(atan(1))),
    sem("Pow", select(pow(2, 3)))
  )

  /**
   * Syntax only: `dictGet*` resolves against the server's configured dictionaries, and a test database has none, so
   * `EXPLAIN QUERY TREE` would report a missing dictionary rather than anything about our rendering.
   */
  private val dictionaryConstructs = Seq(
    syn("DictGetUInt8", select(dictGetUInt8("dict", "attr", 1))),
    syn("DictGetUInt16", select(dictGetUInt16("dict", "attr", 1))),
    syn("DictGetUInt32", select(dictGetUInt32("dict", "attr", 1))),
    syn("DictGetUInt64", select(dictGetUInt64("dict", "attr", 1))),
    syn("DictGetInt8", select(dictGetInt8("dict", "attr", 1))),
    syn("DictGetInt16", select(dictGetInt16("dict", "attr", 1))),
    syn("DictGetInt32", select(dictGetInt32("dict", "attr", 1))),
    syn("DictGetInt64", select(dictGetInt64("dict", "attr", 1))),
    syn("DictGetFloat32", select(dictGetFloat32("dict", "attr", 1))),
    syn("DictGetFloat64", select(dictGetFloat64("dict", "attr", 1))),
    syn("DictGetDate", select(dictGetDate("dict", "attr", 1))),
    syn("DictGetDateTime", select(dictGetDateTime("dict", "attr", 1))),
    syn("DictGetUUID", select(dictGetUUID("dict", "attr", 1))),
    syn("DictGetString", select(dictGetString("dict", "attr", 1))),
    syn("DictIsIn", select(dictIsIn("dict", 1, 2))),
    syn("DictGetHierarchy", select(dictGetHierarchy("dict", 1))),
    syn("DictHas", select(dictHas("dict", 1)))
  )

  /**
   * Regression guards for the tuple/map area. `TupleElement` is the arm that shipped broken; the `*Map` aggregates are
   * the reason it is reachable at all.
   */
  private val tupleAndMapConstructs = Seq(
    sem("SumMap", select(sumMap(numbers, numbers)) from OneTestTable),
    sem("MinMap", select(minMap(numbers, numbers)) from OneTestTable),
    sem("MaxMap", select(maxMap(numbers, numbers)) from OneTestTable),
    sem("TupleElement", select(mapValues(sumMap(numbers, numbers))) from OneTestTable),
    sem("Tuple", select(tupleElement[Int](tuple(1, 2), 1))),
    sem("Array", select(arrayOf(1, 2, 3)))
  )

  private val urlConstructs = {
    val url = "http://www.example.com/a/b?x=1&y=2#frag"
    Seq(
      sem("Protocol", select(protocol(url))),
      sem("Domain", select(domain(url))),
      sem("DomainWithoutWWW", select(domainWithoutWWW(url))),
      sem("TopLevelDomain", select(topLevelDomain(url))),
      sem("FirstSignificantSubdomain", select(firstSignificantSubdomain(url))),
      sem("CutToFirstSignificantSubdomain", select(cutToFirstSignificantSubdomain(url))),
      sem("Path", select(path(url))),
      sem("PathFull", select(pathFull(url))),
      sem("QueryString", select(queryString(url))),
      sem("Fragment", select(fragment(url))),
      sem("QueryStringAndFragment", select(queryStringAndFragment(url))),
      sem("ExtractURLParameter", select(extractURLParameter(url, "x"))),
      sem("ExtractURLParameters", select(extractURLParameters(url))),
      sem("ExtractURLParameterNames", select(extractURLParameterNames(url))),
      sem("URLHierarchy", select(uRLHierarchy(url))),
      sem("URLPathHierarchy", select(uRLPathHierarchy(url))),
      sem("DecodeURLComponent", select(decodeURLComponent("%2Fa%2Fb"))),
      sem("CutWWW", select(cutWWW(url))),
      sem("CutQueryString", select(cutQueryString(url))),
      sem("CutFragment", select(cutFragment(url))),
      sem("CutQueryStringAndFragment", select(cutQueryStringAndFragment(url))),
      sem("CutURLParameter", select(cutURLParameter(url, "x")))
    )
  }

  private val hashConstructs = Seq(
    sem("HalfMD5", select(halfMD5("a"))),
    sem("MD5", select(mD5("a"))),
    sem("SipHash64", select(sipHash64("a"))),
    sem("SipHash128", select(sipHash128("a"))),
    sem("CityHash64", select(cityHash64("a", "b"))),
    sem("IntHash32", select(intHash32(1))),
    sem("IntHash64", select(intHash64(1))),
    sem("SHA1", select(sHA1("a"))),
    sem("SHA224", select(sHA224("a"))),
    sem("SHA256", select(sHA256("a"))),
    sem("URLHash", select(uRLHash("http://example.com/a/b", 1)))
  )

  private val distanceConstructs = {
    val v1 = Seq(1.0, 2.0)
    val v2 = Seq(3.0, 4.0)
    val t1 = tuple(1.0, 2.0)
    Seq(
      sem("L1Norm", select(l1Norm[Double](v1))),
      sem("L1Distance", select(l1Distance[Double](v1, v2))),
      sem("L2Norm", select(l2Norm[Double](v1))),
      sem("L2Distance", select(l2Distance[Double](v1, v2))),
      sem("L2SquaredNorm", select(l2SquaredNorm[Double](v1))),
      sem("L2SquaredDistance", select(l2SquaredDistance[Double](v1, v2))),
      sem("LInfNorm", select(lInfNorm[Double](v1))),
      sem("LInfDistance", select(lInfDistance[Double](v1, v2))),
      sem("LPNorm", select(lPNorm[Double](v1, 2.0f))),
      sem("LPDistance", select(lPDistance[Double](v1, v2, 2.0f))),
      sem("CosineDistance", select(cosineDistance[Double](v1, v2))),
      // The *Normalize functions take one tuple, not two arrays -- see DistanceFunctions.
      sem("L1Normalize", select(l1Normalize(t1))),
      sem("L2Normalize", select(l2Normalize(t1))),
      sem("LInfNormalize", select(lInfNormalize(t1))),
      sem("LPNormalize", select(lPNormalize(t1, 2.0f)))
    )
  }

  private val stringConstructs = Seq(
    sem("Length", select(com.crobox.clickhouse.dsl.length("abc"))),
    sem("LengthUTF8", select(lengthUTF8("abc"))),
    sem("Lower", select(lower("ABC"))),
    sem("Upper", select(upper("abc"))),
    sem("LowerUTF8", select(lowerUTF8("ABC"))),
    sem("UpperUTF8", select(upperUTF8("abc"))),
    sem("Reverse", select(reverse("abc"))),
    sem("ReverseUTF8", select(reverseUTF8("abc"))),
    sem("Concat", select(concat("a", "b", "c"))),
    sem("Substring", select(substring("abcdef", 2, 3))),
    sem("SubstringUTF8", select(substringUTF8("abcdef", 2, 3))),
    sem("AppendTrailingCharIfAbsent", select(appendTrailingCharIfAbsent("abc", "/"))),
    sem("ConvertCharset", select(convertCharset("abc", "UTF-8", "UTF-16")))
  )

  private val stringSearchConstructs = Seq(
    sem("Position", select(position("abc", "b"))),
    sem("Position", select(positionCaseInsensitive("abc", "B"))),
    sem("PositionUTF8", select(positionUTF8("abc", "b"))),
    sem("PositionUTF8", select(positionUTF8CaseInsensitive("abc", "B"))),
    sem("StrMatch", select(strMatch("abc", "b"))),
    sem("Extract", select(extract("abc123", "[0-9]+"))),
    sem("ExtractAll", select(extractAll("abc123", "[0-9]+"))),
    sem("ILike", select(iLike("abc", "%B%"))),
    sem("Like", select(like("abc", "%b%"))),
    sem("NotLike", select(notLike("abc", "%b%"))),
    sem("ReplaceOne", select(replaceOne("abc", "b", "x"))),
    sem("ReplaceAll", select(replaceAll("abc", "b", "x"))),
    sem("ReplaceRegexpOne", select(replaceRegexpOne("abc", "b", "x"))),
    sem("ReplaceRegexpAll", select(replaceRegexpAll("abc", "b", "x")))
  )

  private val miscConstructs = Seq(
    sem("HostName", select(hostName())),
    sem("VisibleWidth", select(visibleWidth(1))),
    sem("ToTypeName", select(toTypeName(1))),
    sem("BlockSize", select(blockSize())),
    sem("Materialize", select(materialize(1))),
    sem("Ignore", select(com.crobox.clickhouse.dsl.ignore(1, 2))),
    sem("Sleep", select(sleep(0))),
    sem("CurrentDatabase", select(currentDatabase())),
    sem("IsFinite", select(isFinite(1.0))),
    sem("IsInfinite", select(isInfinite(1.0))),
    sem("IsNaN", select(isNaN(1.0))),
    sem("HasColumnInTable", select(hasColumnInTable("system", "numbers", "number"))),
    sem("Bar", select(bar(5, 0, 10, None))),
    sem("Transform", select(transform[Int, String](1, Seq(1, 2), Seq("a", "b"), "c"))),
    sem("FormatReadableSize", select(formatReadableSize(1024))),
    sem("Least", select(least(1, 2))),
    sem("Greatest", select(greatest(1, 2))),
    sem("Uptime", select(uptime())),
    sem("Version", select(version())),
    sem("RowNumberInAllBlocks", select(rowNumberInAllBlocks())),
    sem("MACNumToString", select(mACNumToString(toUInt64(1)))),
    sem("MACStringToNum", select(mACStringToNum("01:02:03:04:05:06"))),
    sem("MACStringToOUI", select(mACStringToOUI("01:02:03:04:05:06")))
  )

  private val arithmeticConstructs = Seq(
    sem("Plus", select(plus(1, 2))),
    sem("Minus", select(minus(3, 1))),
    sem("Multiply", select(multiply(2, 3))),
    sem("Divide", select(divide(6, 3))),
    sem("IntDiv", select(intDiv(7, 2))),
    sem("IntDivOrZero", select(intDivOrZero(7, 2))),
    sem("Modulo", select(modulo(7, 2))),
    sem("Power", select(power(2, 3))),
    sem("Gcd", select(gcd(12, 8))),
    sem("Lcm", select(lcm(4, 6))),
    sem("Negate", select(negate(1))),
    sem("Abs", select(abs(-1)))
  )

  private val operatorConstructs = Seq(
    sem("ComparisonColumn", select(isEqual(1, 1))),
    sem("LogicalFunction", select(and(col2 > 1, col2 < 5)) from TwoTestTable),
    sem("Cast", select(cast(1, ColumnType.UInt32))),
    sem("In", select(com.crobox.clickhouse.dsl.in(col2, Seq(1, 2))) from TwoTestTable),
    sem("NotIn", select(notIn(col2, Seq(1, 2))) from TwoTestTable),
    sem("GlobalIn", select(globalIn(col2, Seq(1, 2))) from TwoTestTable),
    sem("GlobalNotIn", select(globalNotIn(col2, Seq(1, 2))) from TwoTestTable)
  )

  private val typeCastConstructs = Seq(
    sem("UInt8", select(toUInt8("1"))),
    sem("UInt16", select(toUInt16("1"))),
    sem("UInt32", select(toUInt32("1"))),
    sem("UInt64", select(toUInt64("1"))),
    sem("Int8", select(toInt8("1"))),
    sem("Int16", select(toInt16("1"))),
    sem("Int32", select(toInt32("1"))),
    sem("Int64", select(toInt64("1"))),
    sem("Float32", select(toFloat32("1"))),
    sem("Float64", select(toFloat64("1"))),
    sem("Uuid", select(toUUID("00000000-0000-0000-0000-000000000000"))),
    sem("DateRep", select(toDate("2020-01-01"))),
    sem("DateTimeRep", select(toDateTime("2020-01-01 00:00:00"))),
    sem("StringRep", select(toStringRep(1))),
    sem("FixedString", select(toFixedString("ab", 2))),
    sem("StringCutToZero", select(toStringCutToZero("ab"))),
    sem("Reinterpret", select(reinterpret[Long](toUInt64("1")))),
    // The Or* variants render a different arm each. OrDefault renders the default value, which went through toString
    // and so emitted literals the server could not parse for every non-numeric type.
    sem("UInt32", select(toUInt32OrZero("x"))),
    sem("UInt32", select(toUInt32OrNull("x"))),
    sem("UInt32", select(toUInt32OrDefault("x", 0))),
    sem("Int16", select(toInt16OrDefault("x", (-7).toShort))),
    sem("Float64", select(toFloat64OrDefault("x", 1.5))),
    sem("DateRep", select(toDateOrDefault("x", stampInAmsterdam))),
    sem("DateTimeRep", select(toDateTimeOrDefault("x", stampInAmsterdam))),
    sem("Uuid", select(toUUIDOrDefault("x", toUUID("00000000-0000-0000-0000-000000000001"))))
  )

  private val dateTimeConstructs = {
    val date  = java.time.LocalDate.of(2020, 1, 5)
    val stamp = java.time.ZonedDateTime.of(2020, 1, 5, 10, 20, 30, 0, java.time.ZoneOffset.UTC)
    Seq(
      sem("Year", select(toYear(date))),
      sem("YYYYMM", select(toYYYYMM(date))),
      sem("Month", select(toMonth(date))),
      sem("DayOfMonth", select(toDayOfMonth(date))),
      sem("DayOfWeek", select(toDayOfWeek(date))),
      sem("Hour", select(toHour(stamp))),
      sem("Minute", select(toMinute(stamp))),
      sem("Second", select(toSecond(stamp))),
      sem("Monday", select(toMonday(date))),
      sem("AddSeconds", select(addSeconds(stamp, 1))),
      sem("AddMinutes", select(addMinutes(stamp, 1))),
      sem("AddHours", select(addHours(stamp, 1))),
      sem("AddDays", select(addDays(date, 1))),
      sem("AddWeeks", select(addWeeks(date, 1))),
      sem("AddMonths", select(addMonths(date, 1))),
      sem("AddYears", select(addYears(date, 1))),
      sem("StartOfMonth", select(toStartOfMonth(date))),
      sem("StartOfQuarter", select(toStartOfQuarter(date))),
      sem("StartOfYear", select(toStartOfYear(date))),
      sem("StartOfMinute", select(toStartOfMinute(stamp))),
      sem("StartOfFiveMinute", select(toStartOfFiveMinute(stamp))),
      sem("StartOfFifteenMinutes", select(toStartOfFifteenMinutes(stamp))),
      sem("StartOfHour", select(toStartOfHour(stamp))),
      sem("StartOfDay", select(toStartOfDay(stamp))),
      sem("Time", select(toTime(stamp))),
      sem("RelativeYearNum", select(toRelativeYearNum(date))),
      sem("RelativeQuarterNum", select(toRelativeQuarterNum(date))),
      sem("RelativeMonthNum", select(toRelativeMonthNum(date))),
      sem("RelativeWeekNum", select(toRelativeWeekNum(date))),
      sem("RelativeDayNum", select(toRelativeDayNum(date))),
      sem("RelativeHourNum", select(toRelativeHourNum(stamp))),
      sem("RelativeMinuteNum", select(toRelativeMinuteNum(stamp))),
      sem("RelativeSecondNum", select(toRelativeSecondNum(stamp))),
      sem("Now", select(chNow())),
      sem("Today", select(chToday())),
      sem("Yesterday", select(chYesterday())),
      sem("TimeSlot", select(timeSlot(stamp))),
      sem("TimeSlots", select(timeSlots(stamp, toUInt32(1800)))),
      sem("ISOYear", select(toISOYear(date))),
      sem("ISOWeek", select(toISOWeek(date))),
      sem("Week", select(toWeek(date)))
    )
  }

  private val aggregationConstructs = Seq(
    sem("Count", select(count()) from TwoTestTable),
    sem("Avg", select(average(col2)) from TwoTestTable),
    sem("Min", select(min(col2)) from TwoTestTable),
    sem("Max", select(max(col2)) from TwoTestTable),
    sem("Sum", select(sum(col2)) from TwoTestTable),
    sem("AnyResult", select(any(col2)) from TwoTestTable),
    sem("Uniq", select(uniq(col2)) from TwoTestTable),
    sem("GroupArray", select(groupArray(col2, Option(3L))) from TwoTestTable),
    sem("GroupUniqArray", select(groupUniqArray(col2)) from TwoTestTable),
    sem("FirstValue", select(firstValue(col2)) from TwoTestTable),
    sem("LastValue", select(lastValue(col2)) from TwoTestTable),
    sem("Quantile", select(quantile(col2, 0.5f)) from TwoTestTable),
    sem("Quantiles", select(quantiles(col2, 0.25f, 0.75f)) from TwoTestTable),
    sem("Median", select(median(col2, 0.5f)) from TwoTestTable),
    sem("Deterministic", select(quantileDeterministic(col2, col2, 0.5f)) from TwoTestTable),
    sem("TimingWeighted", select(quantileTimingWeighted(col2, col2, 0.5f)) from TwoTestTable),
    sem("ExactWeighted", select(quantileExactWeighted(col2, col2, 0.5f)) from TwoTestTable),
    sem(
      "CombinedAggregatedFunction",
      select(aggIf[TableColumn[Double], Double](col2 > 1)(sum(col2))) from TwoTestTable
    ),
    sem("If", select(aggIf[TableColumn[Double], Double](col2 > 1)(sum(col2))) from TwoTestTable),
    sem("CombinatorArray", select(array[TableColumn[Seq[Double]], Double](sum(numbers))) from OneTestTable),
    sem("ArrayForEach", select(forEach[Int, NativeColumn[Seq[Int]], Double](numbers)(sum(_))) from OneTestTable),
    sem("State", select(state[TableColumn[Double], Double](sum(col2))) from TwoTestTable),
    // Syntax only: sumMerge needs an AggregateFunction(sum, ...) column to read, and the test tables hold plain values.
    syn("Merge", select(merge[TableColumn[StateResult[Double]], Double](sum(col2))) from TwoTestTable),
    sem("TimeSeries", select(timeSeries(timestampColumn, oneDay)) from OneTestTable),
    sem("ArgMin", select(argMin(col2, col3)) from TwoTestTable),
    // count(DISTINCT) under a combinator: renders `countIf(DISTINCT column_2, ...)`, which the server accepts and
    // reads as "count the distinct values among the rows matching the condition".
    sem(
      "Count",
      select(aggIf[TableColumn[Long], Long](col2 > 1)(countDistinct(col2))) from TwoTestTable
    ),
    sem("ArgMax", select(argMax(col2, col3)) from TwoTestTable)
  )

  /**
   * Window-only functions, each behind the `OVER` that ClickHouse requires -- without one the server answers
   * BAD_ARGUMENTS, "can only be used as a window function", which is why the builders return something that is not a
   * Column.
   */
  private val windowConstructs = Seq(
    sem("RowNumber", select(rowNumber().over(WindowSpec(orderBy = Seq(col2)))) from TwoTestTable),
    sem("Rank", select(rank().over(WindowSpec(orderBy = Seq(col2)))) from TwoTestTable),
    sem("DenseRank", select(denseRank().over(WindowSpec(orderBy = Seq(col2)))) from TwoTestTable),
    sem("PercentRank", select(percentRank().over(WindowSpec(orderBy = Seq(col2)))) from TwoTestTable),
    sem("Ntile", select(ntile(4).over(WindowSpec(orderBy = Seq(col2)))) from TwoTestTable),
    sem(
      "LagInFrame",
      // The default has to land on column_2's own UInt32; a Float64 one is rejected outright.
      select(lagInFrame(col2, Option(const(1L)), Option(const(0))).over(WindowSpec(orderBy = Seq(col2))))
        from TwoTestTable
    ),
    sem(
      "LeadInFrame",
      select(
        leadInFrame(col2, Option(const(1L)))
          .over(
            WindowSpec(
              orderBy = Seq(col2),
              frame = Option(WindowFrame(FrameMode.Rows, FrameBound.CurrentRow, Option(FrameBound.Following(1))))
            )
          )
      ) from TwoTestTable
    ),
    sem("NthValue", select(nthValue(col2, 2).over(WindowSpec(orderBy = Seq(col2)))) from TwoTestTable)
  )

  /** AST nodes covered above; these are what the coverage ratchet counts. */
  private val astConstructs: Seq[Construct] =
    bitConstructs ++ randomConstructs ++ roundingConstructs ++ mathConstructs ++
      dictionaryConstructs ++ tupleAndMapConstructs ++ nullableConstructs ++ jsonConstructs ++
      encodingConstructs ++ ipConstructs ++ splitMergeConstructs ++ emptyConstructs ++ higherOrderConstructs ++
      arrayConstructs ++ urlConstructs ++ hashConstructs ++ distanceConstructs ++ stringConstructs ++
      stringSearchConstructs ++ miscConstructs ++ arithmeticConstructs ++ operatorConstructs ++ typeCastConstructs ++
      dateTimeConstructs ++ aggregationConstructs ++ windowConstructs

  /**
   * Whole-query shapes rather than individual functions, so they are validated but not counted towards AST coverage.
   *
   * The join shapes are here because of the ClickHouse 24.3 analyzer change: a `USING` key must resolve against the
   * left table expression, so `select(a as b) ... join(t) using b` over a *table* cannot render as `USING b` -- it has
   * to become `ON L.a = R.b`. Over a *subquery* the alias is a real output column and `USING` is still correct. Both
   * directions are pinned here because only a real server can tell them apart.
   */
  private val joinShapes: Seq[Construct] = Seq(
    Construct(
      "join using a projection alias over a table",
      select(shieldId as itemId).from(OneTestTable).join(JoinQuery.InnerJoin, TwoTestTable) using itemId
    ),
    Construct(
      "join using a projection alias over a subquery",
      select(shieldId as itemId)
        .from(select(shieldId).from(OneTestTable))
        .join(JoinQuery.InnerJoin, TwoTestTable) using itemId
    ),
    Construct(
      "join using a real column",
      select(itemId).from(TwoTestTable).join(JoinQuery.InnerJoin, ThreeTestTable) using itemId
    ),
    Construct(
      "join on an explicit condition",
      select(shieldId as itemId).from(OneTestTable).join(JoinQuery.InnerJoin, TwoTestTable) on itemId
    )
  )

  /**
   * Whole-query clause shapes, so they live here rather than in [[astConstructs]] -- the coverage ratchet matches those
   * names against the declarations in `dsl/column`, and a clause has no node there.
   */
  private val clauseShapes: Seq[Construct] = Seq(
    Construct("array join", select(shieldId).from(OneTestTable).withArrayJoin(numbers as "n")),
    Construct("left array join", select(shieldId).from(OneTestTable).withLeftArrayJoin(numbers as "n")),
    Construct(
      "array join alongside a join, where it has to follow the left table's alias",
      select(shieldId as itemId)
        .from(OneTestTable)
        .withArrayJoin(numbers as "n")
        .join(JoinQuery.InnerJoin, TwoTestTable) using itemId
    ),
    Construct("query-level settings", select(shieldId).from(OneTestTable).settings("max_threads" -> "1")),
    Construct(
      "settings before a union",
      select(shieldId).from(OneTestTable).settings("max_threads" -> "1").unionAll(select(shieldId).from(OneTestTable))
    ),
    Construct("paste join", select(itemId).from(TwoTestTable).join(JoinQuery.PasteJoin, ThreeTestTable)),
    // Syntax only: the IT tables are Engine.Memory, and resolving SAMPLE against a table with no sampling key fails
    // with SAMPLING_NOT_SUPPORTED before the analyzer gets to the clause itself.
    Construct(
      "with, naming an expression",
      select(ref[Int]("one")).from(OneTestTable).withCte(WithExpression(const(1), "one"))
    ),
    Construct(
      "with, naming a scalar subquery",
      select(itemId, ref[Long]("total"))
        .from(TwoTestTable)
        .withCte(WithScalarQuery(select(count()).from(TwoTestTable), "total"))
    ),
    Construct(
      "with, declaring a CTE and selecting from it", {
        val recent = WithTable("recent", select(itemId).from(TwoTestTable))
        select(itemId).from(recent).withCte(recent)
      }
    ),
    Construct(
      "with, scoped to a union branch",
      select(ref[Int]("one"))
        .from(OneTestTable)
        .withCte(WithExpression(const(1), "one"))
        .unionAll(select(ref[Int]("two")).from(OneTestTable).withCte(WithExpression(const(2), "two")))
    ),
    Construct(
      "order by with fill",
      select(col2).from(TwoTestTable).orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
    ),
    Construct(
      "order by with fill, bounded",
      select(col2)
        .from(TwoTestTable)
        .orderByColumns(
          OrderingColumn(
            col2,
            ASC,
            Option(WithFill(from = Option(const(1)), to = Option(const(6)), step = Option(const(1))))
          )
        )
    ),
    Construct(
      "order by with fill and staleness, which cannot be combined with FROM",
      select(col2)
        .from(TwoTestTable)
        .orderByColumns(
          OrderingColumn(col2, ASC, Option(WithFill(to = Option(const(6)), staleness = Option(const(3)))))
        )
    ),
    Construct(
      "interpolate, naming a column",
      select(col2, col3)
        .from(TwoTestTable)
        .orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
        .interpolate(InterpolateColumn(col3))
    ),
    Construct(
      "interpolate, bare",
      select(col2, col3)
        .from(TwoTestTable)
        .orderByColumns(OrderingColumn(col2, ASC, Option(WithFill())))
        .interpolate()
    ),
    Construct("numbers", select(com.crobox.clickhouse.dsl.all).from(com.crobox.clickhouse.dsl.numbers(3))),
    Construct(
      "numbers from an offset",
      select(com.crobox.clickhouse.dsl.all).from(com.crobox.clickhouse.dsl.numbers(10, 3))
    ),
    Construct("zeros", select(com.crobox.clickhouse.dsl.all).from(com.crobox.clickhouse.dsl.zeros(3))),
    Construct(
      "values as rows",
      select(com.crobox.clickhouse.dsl.all)
        .from(com.crobox.clickhouse.dsl.values(tuple(const(1), const("x")), tuple(const(2), const("y"))))
    ),
    Construct(
      "values with a structure",
      select(com.crobox.clickhouse.dsl.all).from(valuesWithStructure("a UInt32", const(1), const(2)))
    ),
    Construct("merge", select(com.crobox.clickhouse.dsl.all).from(mergeTables("system", "^one$"))),
    Construct("generateRandom", select(com.crobox.clickhouse.dsl.all).from(generateRandom("a UInt8"))),
    Construct(
      "generateRandom, seeded",
      select(com.crobox.clickhouse.dsl.all).from(generateRandom("a UInt8", 1, 1, 1))
    ),
    Construct("remote", select(count()).from(remote("localhost:9000", "system", "one"))),
    // Syntax only: resolving a cluster reads the server's own configuration, and the test server declares only
    // `default` -- which is a property of the fixture rather than of the rendering.
    syn("cluster", select(count()).from(cluster("test_cluster", "system", "one"))),
    Construct(
      "a table function with an alias",
      select(ref[Long]("number")).from(com.crobox.clickhouse.dsl.numbers(3), "n")
    ),
    Construct(
      "a table function as a join right-hand side",
      select(shieldId).from(OneTestTable).join(JoinQuery.CrossJoin, com.crobox.clickhouse.dsl.numbers(3))
    ),
    Construct(
      "a table function as a subquery source",
      select(com.crobox.clickhouse.dsl.all)
        .from(select(com.crobox.clickhouse.dsl.all).from(com.crobox.clickhouse.dsl.numbers(3)))
    ),
    Construct(
      "order by nulls first",
      select(col2).from(TwoTestTable).orderByColumns(OrderingColumn(col2, DESC, nulls = Option(NullsOrder.NullsFirst)))
    ),
    Construct(
      "order by collate",
      select(col3).from(TwoTestTable).orderByColumns(OrderingColumn(col3, collate = Option("en")))
    ),
    Construct(
      "order by with every modifier, in the one order the grammar accepts",
      select(col2)
        .from(TwoTestTable)
        .orderByColumns(
          OrderingColumn(
            col2,
            DESC,
            fill = Option(WithFill(step = Option(const(1)))),
            nulls = Option(NullsOrder.NullsLast),
            collate = Option("en")
          )
        )
    ),
    Construct("limit with ties", select(col2).from(TwoTestTable).orderBy(col2).limitWithTies(2)),
    Construct(
      "limit with ties and an offset, which only parses as LIMIT offset, size WITH TIES",
      select(col2).from(TwoTestTable).orderBy(col2).limitWithTies(2, 1)
    ),
    Construct("offset without a limit", select(col2).from(TwoTestTable).orderBy(col2).offset(2)),
    Construct(
      "union distinct",
      (select(itemId) from TwoTestTable).unionDistinct(select(itemId) from ThreeTestTable)
    ),
    Construct(
      "intersect",
      (select(itemId) from TwoTestTable).intersect(select(itemId) from ThreeTestTable)
    ),
    Construct(
      "intersect distinct",
      (select(itemId) from TwoTestTable).intersectDistinct(select(itemId) from ThreeTestTable)
    ),
    Construct(
      "except",
      (select(itemId) from TwoTestTable).except(select(itemId) from ThreeTestTable)
    ),
    Construct(
      "except distinct",
      (select(itemId) from TwoTestTable).exceptDistinct(select(itemId) from ThreeTestTable)
    ),
    Construct(
      "except chained with union, which share a precedence level",
      (select(itemId) from TwoTestTable)
        .except(select(itemId) from ThreeTestTable)
        .unionAll(select(itemId) from TwoTestTable)
    ),
    Construct(
      // Projects column_3, which only the left table has: once three tables in the chain expose item_id, selecting it
      // unqualified is ambiguous -- a property of the fixture's schema rather than of the rendering.
      "three tables joined at one level, every join keyed off the first",
      select(col3)
        .from(TwoTestTable)
        .join(JoinQuery.InnerJoin, ThreeTestTable)
        .on(itemId)
        .join(JoinQuery.AllLeftJoin, ThreeTestTable)
        .on(itemId)
    ),
    Construct(
      "a chain of joins using USING",
      select(itemId)
        .from(TwoTestTable)
        .join(JoinQuery.InnerJoin, ThreeTestTable)
        .using(itemId)
        .join(JoinQuery.InnerJoin, TwoTestTable)
        .using(itemId)
    ),
    Construct(
      "array join ahead of a chain of joins",
      select(shieldId as itemId)
        .from(OneTestTable)
        .withArrayJoin(numbers as "n")
        .join(JoinQuery.InnerJoin, TwoTestTable)
        .using(itemId)
        .join(JoinQuery.InnerJoin, ThreeTestTable)
        .using(itemId)
    ),
    Construct(
      "group by a column an aggregate already covers, which is now projected",
      select(sum(col2)).from(TwoTestTable).groupBy(col2)
    ),
    // GROUP BY named its keys rather than rendering them, so an expression key became its argument's name -- or NULL
    // for an arithmetic one, which the server rejects outright.
    Construct(
      "group by a function expression",
      select(count()).from(OneTestTable).groupBy(toStartOfDay(toDateTime(timestampColumn)))
    ),
    Construct(
      "group by an arithmetic expression",
      select(count()).from(OneTestTable).groupBy(intDiv(timestampColumn, 1000))
    ),
    Construct(
      "group by a nested expression",
      select(count()).from(OneTestTable).groupBy(toDateTime(intDiv(toUInt64rNull(shieldId), 1000)))
    ),
    Construct(
      "order by an expression differing from the projection",
      select(sum(col2)).from(TwoTestTable).orderBy(uniq(col2))
    ),
    Construct(
      "order by an alias, which must not be projected a second time",
      select(uniq(col2) as "value").from(TwoTestTable).orderBy(ref[Long]("value"))
    ),
    Construct(
      "group by an alias, which must not be projected a second time",
      select(col3 as "value").from(TwoTestTable).groupBy(ref[String]("value"))
    ),
    Construct("re-aliased column", select((shieldId as "from_pv") as "from_start").from(OneTestTable)),
    Construct(
      "an inner alias referenced from the enclosing query",
      select(ref[String]("from_pv") as "from_start").from(select(shieldId as "from_pv").from(OneTestTable))
    ),
    Construct(
      "date minus date",
      select(java.time.LocalDate.of(2020, 1, 5) - java.time.LocalDate.of(2020, 1, 1))
    ),
    Construct(
      "datetime minus datetime",
      select(
        java.time.ZonedDateTime.of(2020, 1, 2, 0, 0, 0, 0, java.time.ZoneOffset.UTC) -
          java.time.ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, java.time.ZoneOffset.UTC)
      )
    ),
    Construct("window function over an empty window", select(sum(col2).over()).from(TwoTestTable)),
    Construct(
      "window function over a partition and ordering",
      select(sum(col2).over(WindowSpec(partitionBy = Seq(col3), orderBy = Seq(OrderingColumn(col2)))))
        .from(TwoTestTable)
    ),
    Construct(
      "window function with a ROWS frame",
      select(
        sum(col2).over(
          WindowSpec(
            orderBy = Seq(OrderingColumn(col2)),
            frame = Option(WindowFrame(FrameMode.Rows, FrameBound.Preceding(1), Option(FrameBound.CurrentRow)))
          )
        )
      ).from(TwoTestTable)
    ),
    Construct(
      "window function with a RANGE frame",
      select(
        sum(col2).over(
          WindowSpec(
            orderBy = Seq(OrderingColumn(col2)),
            frame = Option(
              WindowFrame(FrameMode.Range, FrameBound.UnboundedPreceding, Option(FrameBound.UnboundedFollowing))
            )
          )
        )
      ).from(TwoTestTable)
    ),
    Construct(
      "a named window", {
        val w = NamedWindow("w", WindowSpec(partitionBy = Seq(col3)))
        select(sum(col2).over(w)).from(TwoTestTable).window(w)
      }
    ),
    Construct(
      "qualify on a window result",
      select(sum(col2).over() as "s").from(TwoTestTable).qualify(ref[Long]("s") > 1)
    ),
    Construct("sample", select(shieldId).from(OneTestTable).sample(0.1), Syntax),
    Construct("sample with an offset", select(shieldId).from(OneTestTable).sample(0.1, Option(0.5)), Syntax)
  )

  private val constructs: Seq[Construct] = astConstructs ++ joinShapes ++ clauseShapes

  /** `EXPLAIN QUERY TREE` needs the analyzer, the default since 24.3 and so on every supported server. */
  private def explainOf(level: ValidationLevel): String = level match {
    case Semantic => "EXPLAIN QUERY TREE "
    case _        => "EXPLAIN AST "
  }

  it should "emit SQL that ClickHouse parses, for every registered construct" in {
    // One at a time. Firing all of them concurrently overruns the client's connection pool
    // (pekko.http.host-connection-pool.max-open-requests, 32 by default) and every result becomes a pool rejection
    // rather than a verdict on the SQL.
    val failures = constructs.flatMap { construct =>
      val sql = toSql(construct.query.internalQuery, None)
      clickClient
        .query(explainOf(construct.level) + sql)
        .map(_ => None)
        .recover { case error => Some((construct, sql, firstLineOf(error))) }
        .futureValue
    }

    if (failures.nonEmpty) {
      val report = failures
        .map { case (construct, sql, message) =>
          s"  ${construct.ast} (${construct.level})\n    emitted: $sql\n    server : $message"
        }
        .mkString("\n")
      fail(s"${failures.size} of ${constructs.size} constructs emitted SQL ClickHouse rejected:\n$report")
    }
  }

  /**
   * Now a gate: every AST case class in `dsl/column` has an entry, so a new function without one fails here.
   */
  it should "have a rendering check for every AST node" in {
    val declared   = declaredAstNames()
    val registered = astConstructs.map(_.ast).toSet

    val unknown = registered.diff(declared)
    withClue(s"registered under names that no longer exist in dsl/column (renamed or misspelled): $unknown\n") {
      unknown shouldBe empty
    }

    val unregistered = declared.diff(registered).toSeq.sorted
    withClue(
      s"${unregistered.size} AST nodes have no entry in this registry. Add one per node above.\n" +
        unregistered.mkString(", ") + "\n"
    ) {
      unregistered.size should be <= SqlValidationITSpec.UnregisteredBaseline
    }
  }

  private def firstLineOf(error: Throwable): String =
    Option(error.getMessage).map(_.linesIterator.next().take(200)).getOrElse(error.getClass.getName)

  /** Simple names of every `case class` declared under `dsl/column`, read from source. */
  private def declaredAstNames(): Set[String] = {
    // Walk up to the repo root -- the working directory is the repo root when tests are unforked and the project
    // directory when they are forked, and that is a build setting we should not depend on here.
    val start    = new java.io.File(".").getCanonicalFile
    val repoRoot = Iterator
      .iterate(Option(start))(_.flatMap(d => Option(d.getParentFile)))
      .takeWhile(_.isDefined)
      .flatten
      .find(d => new java.io.File(d, "build.sbt").isFile)
      .getOrElse(fail(s"Could not find the repository root (no build.sbt at or above $start)"))

    val dir = new java.io.File(repoRoot, "dsl/src/main/scala/com/crobox/clickhouse/dsl/column")
    if (!dir.isDirectory) fail(s"Expected DSL column sources at $dir")

    val declaration = """^\s*(?:final\s+)?case class\s+([A-Za-z0-9_]+)""".r
    dir
      .listFiles(new java.io.FilenameFilter {
        override def accept(unused: java.io.File, name: String): Boolean = name.endsWith(".scala")
      })
      .toSeq
      .flatMap { file =>
        val source = scala.io.Source.fromFile(file, "UTF-8")
        try source.getLines().flatMap(declaration.findFirstMatchIn(_).map(_.group(1))).toList
        finally source.close()
      }
      .toSet
  }
}

object SqlValidationITSpec {

  /**
   * How many AST nodes in `dsl/column` may have no entry in the registry. The backlog this started with (288) is worked
   * off, so it is zero and should stay zero: a new function now needs a rendering check to land.
   */
  val UnregisteredBaseline = 0
}
