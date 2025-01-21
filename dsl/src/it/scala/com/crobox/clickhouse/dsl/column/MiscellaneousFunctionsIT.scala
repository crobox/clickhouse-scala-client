package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl._
import com.crobox.clickhouse.{DslITSpec, dsl => CHDsl}

class MiscellaneousFunctionsIT extends DslITSpec {

  it should "succeed for MiscFunctions" in {
    val inf = const(1) / 0

    r(hostName()).length should be > 4
    r(visibleWidth("1")) shouldBe "1"
    r(toTypeName(toUInt64(1))) shouldBe "UInt64"
    r(blockSize()) shouldBe "1"
    r(materialize(1)) shouldBe "1"
    r(CHDsl.ignore()) shouldBe "0"
    r(sleep(0.1)) shouldBe "0"
    r(currentDatabase()) shouldBe "default"
    r(isFinite(inf)) shouldBe "0"
    r(isInfinite(inf)) shouldBe "1"
    r(isNaN(0)) shouldBe "0"
    r(hasColumnInTable("system", "one", "dummy")) shouldBe "1"
    r(transform[Int, String](1, Seq(3, 2, 1), Seq("do", "re", "mi"), "fa")) shouldBe "mi"
    r(formatReadableSize(1)) shouldBe "1.00 B"
    r(least(3, 2)) shouldBe "2"
    r(greatest(3, 2)) shouldBe "3"
    r(uptime()).length should be > 0
    r(version()).length should be > 4
    r(rowNumberInAllBlocks()) shouldBe "0"
    r(runningDifference(1)) shouldBe "0"
    r(mACNumToString(toUInt64(123))) shouldBe "00:00:00:00:00:7B"
    r(mACStringToNum("00:00:00:00:00:7B")) shouldBe "123"
    r(mACStringToOUI("00:00:00:00:00:7B")) shouldBe "0"
  }
}
