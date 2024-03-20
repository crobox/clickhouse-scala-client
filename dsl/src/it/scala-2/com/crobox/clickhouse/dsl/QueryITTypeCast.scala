package com.crobox.clickhouse.dsl

import com.crobox.clickhouse.DslITSpec
class QueryITTypeCast extends DslITSpec {

  // This test uses extension methods removed in Scala 3. Hence only making it available for Scala 2
  it should "perform typecasts" in {

    type TakeIntGiveIntTypes = Column => (TypeCastColumn[T] with Reinterpretable) forSome {
      type T >: Long with String with Float with Serializable
    }

    val takeIntGiveIntCast = Set(
      toUInt8 _,
      toUInt16 _,
      toUInt32 _,
      toUInt64 _,
      toInt8 _,
      toInt16 _,
      toInt32 _,
      toInt64 _,
      toFloat32 _,
      toFloat64 _
    )

    val takeIntGiveStringCast = Set(
      toDate _,
      toDateTime _,
      toStringRep _
    )

    val reinterpToIntCast = takeIntGiveIntCast

    val reinterpToStringCast = Set(
      toStringRep _
    )

    val takeStringGiveIntCast = Set(
      toUInt8OrZero _,
      toUInt16OrZero _,
      toUInt32OrZero _,
      toUInt64OrZero _,
      toInt8OrZero _,
      toInt16OrZero _,
      toInt32OrZero _,
      toInt64OrZero _,
      toFloat32OrZero _,
      toFloat64OrZero _,
      (col: TableColumn[_]) => toFixedString(col, 10),
      toStringCutToZero _
    )
  }
}
