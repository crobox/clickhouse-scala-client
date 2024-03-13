package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Const, TableColumn}
import com.crobox.clickhouse.dsl.marshalling.QueryValueFormats._

trait ScalaBooleanFunctions {
  self: Magnets with ComparisonFunctions =>

  trait ScalaBooleanFunctionOps {
    self: ConstOrColMagnet[_] =>

    def isFalse: TableColumn[Boolean] = _equals(self, Const(false))

    def isTrue: TableColumn[Boolean] = _equals(self, Const(true))
  }

}
