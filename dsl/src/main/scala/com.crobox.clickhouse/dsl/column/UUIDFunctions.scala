package com.crobox.clickhouse.dsl.column

import com.crobox.clickhouse.dsl.{Column, ExpressionColumn}

trait UUIDFunctions { self: Magnets =>

  abstract class UUIDFunction[+V](val innerCol: Column) extends ExpressionColumn[V](innerCol)
}
