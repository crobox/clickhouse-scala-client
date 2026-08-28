package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{isEqual, Const, ExpressionColumn}
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._

trait ScalaBooleanFunctions {
  self: Magnets with ComparisonFunctions =>

  trait ScalaBooleanFunctionOps {
    self: ConstOrColMagnet[_] =>

    def isFalse: ExpressionColumn[Boolean] = isEqual(self, Const(false))

    def isTrue: ExpressionColumn[Boolean] = isEqual(self, Const(true))
  }

}
