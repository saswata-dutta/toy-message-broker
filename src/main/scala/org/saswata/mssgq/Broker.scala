package org.saswata.mssgq

import java.util.concurrent.Semaphore

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class Broker(consumers: Seq[Consumer], capacity: Int, retry: Int) {
  require(capacity > 0, "Capacity Empty")
  require(retry >= 0, "Retry Empty")

  private val consumerIndex = consumers.map(it => (it.id, it)).toMap
  private val consumeOrder: Seq[Consumer] =
    Broker.toposort(consumers).right.getOrElse(Seq.empty[Int]).map(consumerIndex)

  val empty = new Semaphore(capacity, true)
  val full = new Semaphore(0, true)

  def submit(message: String): Boolean = {
    // return false if message is invalid
    if (message == null || message.trim.isEmpty) return false

    // wait emptyCell
    empty.acquire()
    // signal filledCell
    full.release()
    // spawn a push in another thread
    implicit val ec = ExecutionContext.global
    Future {
      push(message)
    }
    true
  }

  private def push(message: String): Unit = {
    // wait filledCell
    full.acquire()
    // pop and process mssg
    process(message)
    // signal emptyCell
    empty.release()
  }

  private def process(message: String): Unit = {
    val visited = mutable.Set[Int]()
    consumeOrder.foreach { consumer =>
      if (consumer.canConsume(message) && consumer.parents.forall(visited)) {
        var retriesLeft = retry
        var done = false
        while (retriesLeft > 0 && !done) {
          if (consumer.consume(message)) {
            done = true
            visited += consumer.id
          } else {
            retriesLeft -= 1
            println(s"retry ${retry - retriesLeft} : $message for ${consumer.id}")
            // maybe pause after a retry here
          }
        }
      }
    }
  }

}

object Broker {

  case class Edge(src: Int, dst: Int)

  def incomingEdges(consumer: Consumer): Seq[Edge] =
    consumer.parents.map(it => Edge(it, consumer.id)).toSeq

  def toposort(consumers: Seq[Consumer]): Either[Set[Int], Seq[Int]] = {
    val edges = consumers.flatMap(incomingEdges)
    var (neighbors, inDegrees, nodes) = makeGraph(edges)
    val sources = mutable.Queue[Int](independentSources(inDegrees): _*)

    val topoOrder = mutable.Buffer[Int]()
    while (sources.nonEmpty) {
      val source = sources.dequeue
      topoOrder += source

      val (newInDegrees, newSources) =
        decrementInDegrees(inDegrees, neighbors.getOrElse(source, Set.empty[Int]))

      inDegrees = newInDegrees
      sources ++= newSources
    }
    if (topoOrder.length < nodes.size)
      Left(nodes -- topoOrder.toSet)
    else Right(topoOrder)
  }

  def makeGraph(edges: Seq[Edge]): (Map[Int, Set[Int]], Map[Int, Int], Set[Int]) =
    edges.foldLeft((Map.empty[Int, Set[Int]], Map.empty[Int, Int], Set.empty[Int])) {
      case ((neighbors, inDegrees, nodes), edge) =>
        val inDegreeDst = inDegrees.getOrElse(edge.dst, 0) + 1
        val inDegreeSrc = inDegrees.getOrElse(edge.src, 0)
        val inDegrees1 = inDegrees + (edge.dst -> inDegreeDst) + (edge.src -> inDegreeSrc)

        val newNeighbors = neighbors.getOrElse(edge.src, Set.empty[Int]) + edge.dst
        val neighbors1 = neighbors + (edge.src -> newNeighbors)
        (neighbors1, inDegrees1, nodes ++ Set(edge.src, edge.dst))
    }

  def independentSources(inDegrees: Map[Int, Int]): Seq[Int] =
    inDegrees.filter { case (_, v) => v < 1 }.keys.toSeq

  def decrementInDegrees(
    inDegrees: Map[Int, Int],
    dependants: Set[Int]
  ): (Map[Int, Int], Set[Int]) =
    dependants.foldLeft(inDegrees, Set.empty[Int]) {
      case ((inDegs, newSources), dep) =>
        val deg = inDegs.getOrElse(dep, 0)
        if (deg == 1) {
          (inDegs + (dep -> 0), newSources + dep)
        } else (inDegs + (dep -> (deg - 1)), newSources)
    }
}
