import scala.actors._
import scala.util.Random

class Node extends Actor {
  private var neighbors: Array[Node] = null
  private val rand = new Random
  private def randomNeighbor: Node = neighbors(rand.nextInt(neighbors.length))
  def act() {
    loop {
      react {
        case n: Array[Node] =>
          neighbors = n
          // set different seeds so that different nodes generate different random number sequences
          rand.setSeed(System.currentTimeMillis() ^ neighbors.hashCode toLong)
        case msg: String =>
          randomNeighbor ! msg
          println(msg)
      }
    }
  }
}

abstract class NetworkBuilder(val numNodes: Int) extends Actor {
  val nodes = Array.fill(numNodes)(new Node)
  val rand = new Random(System.currentTimeMillis())
  def randomNode: Node = nodes(rand.nextInt(nodes.length))
  def neighbors(x: Int): Array[Node]
  def act() {
    nodes foreach(_.start)
    (0 until numNodes) foreach (x => (nodes(x) ! neighbors(x)))
    randomNode ! "start"
  }
}

class Full(numNodes: Int) extends NetworkBuilder(numNodes) {
  def neighbors(x: Int): Array[Node] =
    (0 until numNodes).toArray filter (_!=x) map (nodes(_))
}

class Line(numNodes: Int) extends NetworkBuilder(numNodes) {
  private def inRange(x: Int): Boolean =
    x>=0 && x<numNodes
  def neighbors(x: Int): Array[Node] =
    Array(x-1, x+1) filter (inRange(_)) map (nodes(_))
}

class Grid(val rows: Int, val cols: Int) extends NetworkBuilder(rows*cols) {
  private def at(r: Int, c: Int): Node = nodes(r*cols+c)
  private def inRange(r: Int, c: Int): Boolean =
    r>=0 && c>=0 && r<rows && c<cols
  private def neighbors(r: Int, c: Int): Array[Node] = (
    Array((r-1,c), (r,c-1), (r+1,c), (r,c+1))
    filter{case (r, c) => inRange(r, c)}
    map{case (r, c) => at(r,c)}
  )
  def neighbors(x: Int): Array[Node] =
    neighbors(x/cols, x%cols)
}

class ImperfectGrid(rows: Int, cols: Int) extends Grid(rows, cols) {
  override def neighbors(x: Int): Array[Node] =
    super.neighbors(x) :+ randomNode    // adding a random neighbor
}

object project2 extends App {
  val l = new Line(10)
  l.start
}
