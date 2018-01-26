package com.crobox.clickhouse.balancing.iterator

import scala.collection.{mutable, AbstractIterator}

class CircularIteratorSet[T](seed: Seq[T] = Seq.empty) extends AbstractIterator[T] {

  private var internalIterator         = Iterator.continually(seed).flatten
  private val elements: mutable.Set[T] = mutable.HashSet(seed: _*)

  def add(element: T) = {
    elements += element
    internalIterator = Iterator.continually(elements).flatten
  }

  def remove(element: T) = {
    elements -= element
    internalIterator = Iterator.continually(elements).flatten
  }

  override def hasNext = elements.nonEmpty

  override def next() = internalIterator.next()
}
