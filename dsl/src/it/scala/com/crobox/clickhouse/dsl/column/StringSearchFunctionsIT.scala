package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.DslITSpec
import com.crobox.clickhouse.dsl._

class StringSearchFunctionsIT extends DslITSpec {

  it should "stringFunction: ExtractAll" in {
    execute(select(extractAll("women,unisex", "([^,]+)"))).futureValue should be("['women','unisex']")

    execute(select(arrayReverse(extractAll("women,unisex", "([^,]+)")))).futureValue should be("['unisex','women']")
  }

  it should "iContains" in {
    execute(select("women,unisex".iContains("MeN"))).futureValue should be("1")
    execute(select("women,unisex".iContains("men"))).futureValue should be("1")
    execute(select("women,unisex".iContains("n,UNIs"))).futureValue should be("1")
  }

  it should "iLike" in {
    execute(select(iLike("women,unisex", "womEn,unisex"))).futureValue should be("1")
    execute(select(iLike("women,unisex", "wOmen,unISEx"))).futureValue should be("1")
    execute(select(iLike("women,unisex", "women,UNIsex"))).futureValue should be("1")
    execute(select(iLike("women,unisex", "men,sex"))).futureValue should be("0")
  }
}
