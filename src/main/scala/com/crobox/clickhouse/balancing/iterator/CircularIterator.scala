package com.crobox.clickhouse.balancing.iterator

import scala.collection.{AbstractIterator, mutable}

class CircularIterator[T](seed: Seq[T] = Seq.empty)
    extends AbstractIterator[T] {

  private var internalIterator = Iterator.continually(seed).flatten
  private val elements: mutable.ListBuffer[T] = mutable.ListBuffer(seed: _*)
  def add(element: T) = {
    elements += element
    internalIterator = Iterator.continually(elements).flatten
  }

  def remove(element: T) = {
    elements -= element
    internalIterator = Iterator.continually(elements).flatten
  }

  override def hasNext = internalIterator.hasNext

  override def next() = internalIterator.next()
}
