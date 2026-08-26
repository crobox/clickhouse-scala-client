package com.crobox.clickhouse.dsl

/**
 * The server's `FixedString(N)`: bytes rather than text. It has no values -- it exists so that the functions returning
 * one (`IPv6StringToNum`, `UUIDStringToNum`) only fit the functions that accept one, which reject a plain `String`.
 */
sealed trait FixedStringBytes
