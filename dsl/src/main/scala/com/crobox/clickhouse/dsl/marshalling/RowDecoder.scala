package com.crobox.clickhouse.dsl.marshalling

import spray.json.JsValue

import scala.annotation.implicitNotFound

/**
 * Decodes one result row, positionally, into `R`.
 *
 * Positional rather than by field name is the whole point: with `FORMAT JSON` the only thing tying a query to its
 * decoder was a string -- your `as "x"` alias had to match the field name in a hand-written `jsonFormat(..., "x")`, and
 * nothing checked it. Reading `JSONCompactEachRowWithNamesAndTypes` by position removes that coupling entirely.
 */
@implicitNotFound("No RowDecoder for ${R}. Every column type needs a ColumnDecoder in scope.")
trait RowDecoder[R] {

  /** How many columns this expects, checked against the row before decoding. */
  def arity: Int

  def decode(row: Vector[JsValue], columnNames: Vector[String]): R
}

object RowDecoder {

  def apply[R](implicit decoder: RowDecoder[R]): RowDecoder[R] = decoder

  private def at[A](row: Vector[JsValue], names: Vector[String], index: Int)(implicit
      decoder: ColumnDecoder[A]
  ): A =
    try decoder.decode(row(index))
    catch {
      case cause: ColumnDecodingException =>
        val name = names.lift(index).getOrElse(s"column ${index + 1}")
        throw RowDecodingException(
          s"Could not decode column '$name' (position ${index + 1}): ${cause.getMessage}",
          cause
        )
    }

  /** A single-column result decodes to the value itself rather than to a Tuple1. */
  implicit def single[A](implicit decoder: ColumnDecoder[A]): RowDecoder[A] =
    new RowDecoder[A] {
      val arity                                                  = 1
      def decode(row: Vector[JsValue], names: Vector[String]): A = at[A](row, names, 0)
    }

  implicit def tuple2[A: ColumnDecoder, B: ColumnDecoder]: RowDecoder[(A, B)] =
    new RowDecoder[(A, B)] {
      val arity                                                       = 2
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B) =
        (at[A](row, names, 0), at[B](row, names, 1))
    }
  implicit def tuple3[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder]: RowDecoder[(A, B, C)] =
    new RowDecoder[(A, B, C)] {
      val arity                                                          = 3
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B, C) =
        (at[A](row, names, 0), at[B](row, names, 1), at[C](row, names, 2))
    }
  implicit def tuple4[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder]
      : RowDecoder[(A, B, C, D)] =
    new RowDecoder[(A, B, C, D)] {
      val arity                                                             = 4
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B, C, D) =
        (at[A](row, names, 0), at[B](row, names, 1), at[C](row, names, 2), at[D](row, names, 3))
    }
  implicit def tuple5[A: ColumnDecoder, B: ColumnDecoder, C: ColumnDecoder, D: ColumnDecoder, E: ColumnDecoder]
      : RowDecoder[(A, B, C, D, E)] =
    new RowDecoder[(A, B, C, D, E)] {
      val arity                                                                = 5
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B, C, D, E) =
        (at[A](row, names, 0), at[B](row, names, 1), at[C](row, names, 2), at[D](row, names, 3), at[E](row, names, 4))
    }
  implicit def tuple6[
      A: ColumnDecoder,
      B: ColumnDecoder,
      C: ColumnDecoder,
      D: ColumnDecoder,
      E: ColumnDecoder,
      F: ColumnDecoder
  ]: RowDecoder[(A, B, C, D, E, F)] =
    new RowDecoder[(A, B, C, D, E, F)] {
      val arity                                                                   = 6
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B, C, D, E, F) =
        (
          at[A](row, names, 0),
          at[B](row, names, 1),
          at[C](row, names, 2),
          at[D](row, names, 3),
          at[E](row, names, 4),
          at[F](row, names, 5)
        )
    }
  implicit def tuple7[
      A: ColumnDecoder,
      B: ColumnDecoder,
      C: ColumnDecoder,
      D: ColumnDecoder,
      E: ColumnDecoder,
      F: ColumnDecoder,
      G: ColumnDecoder
  ]: RowDecoder[(A, B, C, D, E, F, G)] =
    new RowDecoder[(A, B, C, D, E, F, G)] {
      val arity                                                                      = 7
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B, C, D, E, F, G) =
        (
          at[A](row, names, 0),
          at[B](row, names, 1),
          at[C](row, names, 2),
          at[D](row, names, 3),
          at[E](row, names, 4),
          at[F](row, names, 5),
          at[G](row, names, 6)
        )
    }
  implicit def tuple8[
      A: ColumnDecoder,
      B: ColumnDecoder,
      C: ColumnDecoder,
      D: ColumnDecoder,
      E: ColumnDecoder,
      F: ColumnDecoder,
      G: ColumnDecoder,
      H: ColumnDecoder
  ]: RowDecoder[(A, B, C, D, E, F, G, H)] =
    new RowDecoder[(A, B, C, D, E, F, G, H)] {
      val arity                                                                         = 8
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B, C, D, E, F, G, H) =
        (
          at[A](row, names, 0),
          at[B](row, names, 1),
          at[C](row, names, 2),
          at[D](row, names, 3),
          at[E](row, names, 4),
          at[F](row, names, 5),
          at[G](row, names, 6),
          at[H](row, names, 7)
        )
    }
  implicit def tuple9[
      A: ColumnDecoder,
      B: ColumnDecoder,
      C: ColumnDecoder,
      D: ColumnDecoder,
      E: ColumnDecoder,
      F: ColumnDecoder,
      G: ColumnDecoder,
      H: ColumnDecoder,
      I: ColumnDecoder
  ]: RowDecoder[(A, B, C, D, E, F, G, H, I)] =
    new RowDecoder[(A, B, C, D, E, F, G, H, I)] {
      val arity                                                                            = 9
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B, C, D, E, F, G, H, I) =
        (
          at[A](row, names, 0),
          at[B](row, names, 1),
          at[C](row, names, 2),
          at[D](row, names, 3),
          at[E](row, names, 4),
          at[F](row, names, 5),
          at[G](row, names, 6),
          at[H](row, names, 7),
          at[I](row, names, 8)
        )
    }
  implicit def tuple10[
      A: ColumnDecoder,
      B: ColumnDecoder,
      C: ColumnDecoder,
      D: ColumnDecoder,
      E: ColumnDecoder,
      F: ColumnDecoder,
      G: ColumnDecoder,
      H: ColumnDecoder,
      I: ColumnDecoder,
      J: ColumnDecoder
  ]: RowDecoder[(A, B, C, D, E, F, G, H, I, J)] =
    new RowDecoder[(A, B, C, D, E, F, G, H, I, J)] {
      val arity                                                                               = 10
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B, C, D, E, F, G, H, I, J) =
        (
          at[A](row, names, 0),
          at[B](row, names, 1),
          at[C](row, names, 2),
          at[D](row, names, 3),
          at[E](row, names, 4),
          at[F](row, names, 5),
          at[G](row, names, 6),
          at[H](row, names, 7),
          at[I](row, names, 8),
          at[J](row, names, 9)
        )
    }
  implicit def tuple11[
      A: ColumnDecoder,
      B: ColumnDecoder,
      C: ColumnDecoder,
      D: ColumnDecoder,
      E: ColumnDecoder,
      F: ColumnDecoder,
      G: ColumnDecoder,
      H: ColumnDecoder,
      I: ColumnDecoder,
      J: ColumnDecoder,
      K: ColumnDecoder
  ]: RowDecoder[(A, B, C, D, E, F, G, H, I, J, K)] =
    new RowDecoder[(A, B, C, D, E, F, G, H, I, J, K)] {
      val arity                                                                                  = 11
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B, C, D, E, F, G, H, I, J, K) =
        (
          at[A](row, names, 0),
          at[B](row, names, 1),
          at[C](row, names, 2),
          at[D](row, names, 3),
          at[E](row, names, 4),
          at[F](row, names, 5),
          at[G](row, names, 6),
          at[H](row, names, 7),
          at[I](row, names, 8),
          at[J](row, names, 9),
          at[K](row, names, 10)
        )
    }
  implicit def tuple12[
      A: ColumnDecoder,
      B: ColumnDecoder,
      C: ColumnDecoder,
      D: ColumnDecoder,
      E: ColumnDecoder,
      F: ColumnDecoder,
      G: ColumnDecoder,
      H: ColumnDecoder,
      I: ColumnDecoder,
      J: ColumnDecoder,
      K: ColumnDecoder,
      L: ColumnDecoder
  ]: RowDecoder[(A, B, C, D, E, F, G, H, I, J, K, L)] =
    new RowDecoder[(A, B, C, D, E, F, G, H, I, J, K, L)] {
      val arity                                                                                     = 12
      def decode(row: Vector[JsValue], names: Vector[String]): (A, B, C, D, E, F, G, H, I, J, K, L) =
        (
          at[A](row, names, 0),
          at[B](row, names, 1),
          at[C](row, names, 2),
          at[D](row, names, 3),
          at[E](row, names, 4),
          at[F](row, names, 5),
          at[G](row, names, 6),
          at[H](row, names, 7),
          at[I](row, names, 8),
          at[J](row, names, 9),
          at[K](row, names, 10),
          at[L](row, names, 11)
        )
    }
}

case class RowDecodingException(message: String, cause: Throwable) extends RuntimeException(message, cause)
