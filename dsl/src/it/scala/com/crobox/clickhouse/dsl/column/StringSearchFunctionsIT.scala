package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._

class StringSearchFunctionsIT extends DslITSpec {

  it should "stringFunction: ExtractAll" in {
    execute(select(extractAll("women,unisex", "([^,]+)"))).futureValue should be("['women','unisex']")

    execute(select(arrayReverse(extractAll("women,unisex", "([^,]+)")))).futureValue should be("['unisex','women']")
  }
}
