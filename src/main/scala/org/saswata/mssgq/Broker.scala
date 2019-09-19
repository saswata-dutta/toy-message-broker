package org.saswata.mssgq

import scala.collection.mutable

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
