import org.saswata.mssgq.{Broker, Consumer}

object Main {

  def main(args: Array[String]): Unit =
    testToposort()

  private val consumers = Seq(
    new Consumer(7, Set.empty[Int], (_: String) => true, (_: String) => true),
    new Consumer(1, Set.empty[Int], (_: String) => true, (_: String) => true),
    new Consumer(2, Set(7, 1), (_: String) => true, (_: String) => true),
    new Consumer(3, Set(1), (_: String) => true, (_: String) => true),
    new Consumer(5, Set(2, 3), (_: String) => true, (_: String) => true),
    new Consumer(4, Set(3), (_: String) => true, (_: String) => true),
    new Consumer(6, Set(4, 5), (_: String) => true, (_: String) => true),
    new Consumer(8, Set(6), (_: String) => true, (_: String) => true)
  )

  def testToposort(): Unit =
    println(Broker.toposort(consumers))
}
