package com.crobox.clickhouse.dsl.column
import com.crobox.clickhouse.dsl.TableColumn

trait ScalaBooleanFunctions { self: Magnets with ComparisonFunctions =>
  trait ScalaBooleanFunctionOps[C] { self: ConstOrColMagnet[C] =>
    def isFalse(): TableColumn[Boolean] = _equals(self, false)
    def isTrue(): TableColumn[Boolean]  = _equals(self, true)
  }
}
