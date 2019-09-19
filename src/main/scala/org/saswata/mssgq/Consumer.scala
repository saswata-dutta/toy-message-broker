package org.saswata.mssgq

class Consumer(
  val id: Int,
  val parents: Set[Int],
  val canConsume: String => Boolean,
  val consume: String => Boolean
)
