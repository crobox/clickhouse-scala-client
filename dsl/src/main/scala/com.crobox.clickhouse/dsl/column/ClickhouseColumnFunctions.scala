package com.crobox.clickhouse.dsl.column

trait ClickhouseColumnFunctions
  extends Magnets
  with AggregationFunctions
  with ArithmeticFunctions
  with ArrayFunctions
  with ComparisonFunctions
  with DateTimeFunctions
  with HigherOrderFunctions
  with TypeCastFunctions
  with StringFunctions




object ClickhouseColumnFunctions extends ClickhouseColumnFunctions{}