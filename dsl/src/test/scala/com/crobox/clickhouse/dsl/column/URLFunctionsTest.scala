package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._

class URLFunctionsTest extends ColumnFunctionTest {

  "Tokenization" should "succeed for URLFunctions" in {
    val someUrl = "https://www.lib.crobox.com/clickhouse/dsl/home.html?search=true#123"
    val someEncodedUrl = "http://127.0.0.1:8123/?query=SELECT%201%3B"
    r(protocol(someUrl)) shouldBe "https"
    r(domain(someUrl)) shouldBe "www.lib.crobox.com"
    r(domainWithoutWWW(someUrl)) shouldBe "lib.crobox.com"
    r(topLevelDomain(someUrl)) shouldBe "com"
    r(firstSignificantSubdomain(someUrl)) shouldBe "crobox"
    r(cutToFirstSignificantSubdomain(someUrl)) shouldBe "crobox.com"
    r(path(someUrl)) shouldBe "/clickhouse/dsl/home.html"
    r(pathFull(someUrl)) shouldBe "/clickhouse/dsl/home.html?search=true#123"
    r(queryString(someUrl)) shouldBe "search=true"
    r(fragment(someUrl)) shouldBe "123"
    r(queryStringAndFragment(someUrl)) shouldBe "search=true#123"
    r(extractURLParameter(someUrl,"search")) shouldBe "true"
    r(extractURLParameters(someUrl)) shouldBe "['search=true']"
    r(extractURLParameterNames(someUrl)) shouldBe "['search']"
    r(uRLHierarchy(someUrl)) shouldBe "[" +
      "'https://www.lib.crobox.com/'," +
      "'https://www.lib.crobox.com/clickhouse/'," +
      "'https://www.lib.crobox.com/clickhouse/dsl/'," +
      "'https://www.lib.crobox.com/clickhouse/dsl/home.html?'," +
      "'https://www.lib.crobox.com/clickhouse/dsl/home.html?search=true#'," +
      "'https://www.lib.crobox.com/clickhouse/dsl/home.html?search=true#123']"
    r(uRLPathHierarchy(someUrl)) shouldBe  "[" +
      "'/clickhouse/'," +
      "'/clickhouse/dsl/'," +
      "'/clickhouse/dsl/home.html?'," +
      "'/clickhouse/dsl/home.html?search=true#'," +
      "'/clickhouse/dsl/home.html?search=true#123']"
    r(decodeURLComponent(someEncodedUrl)) shouldBe "http://127.0.0.1:8123/?query=SELECT 1;"
    r(cutWWW(someUrl)) shouldBe "https://lib.crobox.com/clickhouse/dsl/home.html?search=true#123"
    r(cutQueryString(someUrl)) shouldBe "https://www.lib.crobox.com/clickhouse/dsl/home.html#123"
    r(cutFragment(someUrl)) shouldBe "https://www.lib.crobox.com/clickhouse/dsl/home.html?search=true"
    r(cutQueryStringAndFragment(someUrl)) shouldBe "https://www.lib.crobox.com/clickhouse/dsl/home.html"
    r(cutURLParameter(someUrl,"search")) shouldBe "https://www.lib.crobox.com/clickhouse/dsl/home.html?#123"
  }

}
