package com.crobox.clickhouse.dsl.column

trait ClickhouseColumnFunctions
    extends Magnets
    with AggregationFunctions
    with SumFunctions
    with AnyResultFunctions
    with UniqFunctions
    with Leveled
    with AggregationFunctionsCombiners
    with ArithmeticFunctions
    with ArrayFunctions
    with BitFunctions
    with ComparisonFunctions
    with DateTimeFunctions
    with DictionaryFunctions
    with EmptyFunctions
    with EncodingFunctions
    with HashFunctions
    with HigherOrderFunctions
    with InFunctions
    with IPFunctions
    with JsonFunctions
    with LogicalFunctions
    with MathematicalFunctions
    with MiscellaneousFunctions
    with RandomFunctions
    with RoundingFunctions
    with SplitMergeFunctions
    with ScalaBooleanFunctions
    with ScalaStringFunctions
    with StringFunctions
    with StringSearchFunctions
    with TypeCastFunctions
    with URLFunctions
    with UUIDFunctions

object ClickhouseColumnFunctions extends ClickhouseColumnFunctions {}
