import org.saswata.mssgq.{Broker, Consumer}

object Main {

  def main(args: Array[String]): Unit = {
    val broker = new Broker(consumers, 5, 3)
    broker.process("mssg 1")
  }

  private def consumeSuccess(mssg: String, id: Int): Boolean = {
    println(s"$id consumed $mssg")
    true
  }

  private def consumeFail(mssg: String, id: Int): Boolean = {
    println(s"$id failed $mssg")
    false
  }

  private val consumers = Seq(
    new Consumer(7, Set.empty[Int], (_: String) => true, m => consumeSuccess(m, 7)),
    new Consumer(1, Set.empty[Int], (_: String) => true, m => consumeSuccess(m, 1)),
    new Consumer(2, Set(7, 1), (_: String) => true, m => consumeSuccess(m, 2)),
    new Consumer(3, Set(1), (_: String) => true, m => consumeSuccess(m, 3)),
    new Consumer(5, Set(2, 3), (_: String) => false, m => consumeSuccess(m, 5)),
    new Consumer(4, Set(3), (_: String) => true, m => consumeSuccess(m, 4)),
    new Consumer(6, Set(4, 5), (_: String) => true, m => consumeSuccess(m, 6)),
    new Consumer(8, Set(6), (_: String) => true, m => consumeSuccess(m, 8))
  )

  def testToposort(): Unit =
    println(Broker.toposort(consumers))
}
