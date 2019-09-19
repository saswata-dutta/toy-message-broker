package org.saswata.mssgq

class Producer(broker: Broker) {
  private var counter = 0
  private def message(): String = {
    counter += 1
    s"Message $counter"
  }

  def produce(): Unit = broker.submit(message())
}
