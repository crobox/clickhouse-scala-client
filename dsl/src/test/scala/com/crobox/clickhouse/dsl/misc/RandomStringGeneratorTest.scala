package com.crobox.clickhouse.dsl.misc

import com.crobox.clickhouse.DslTestSpec

class RandomStringGeneratorTest extends DslTestSpec {

  it should "generate of specific length" in {

    (1 to 20).foreach(i => {
      RandomStringGenerator.random(i) should have length (i)
    })
  }

  it should "avoid doubles of equal length" in {
    val n = 1000000
//    (1 to n).map(i => RandomStringGenerator.random(6)).toSet should have size(n)
    (1 to n).map(i => RandomStringGenerator.random(6)).toSet.size should be(n +- 50)
  }
}
