import scala.actors._
import scala.util.Random

class Node extends Actor {
  private var id: Int = 0
  private var neighbors: Array[Node] = null
  private var count: Int = 0
  private val rand = new Random
  private def randomNeighbor: Node = neighbors(rand.nextInt(neighbors.length))
  def act() {
    loop {
      if(count>0) randomNeighbor ! "start"  // count>0 means having been notified
      react {
        case (i: Int, n: Array[Node]) =>
          id = i; neighbors = n
          //set different seeds so that different nodes generate different random number sequences
          rand.setSeed(System.currentTimeMillis() ^ neighbors.hashCode toLong)
        case msg: String =>
          count += 1
          println(""+id+": "+count)
          if(count>=10) exit
      }
    }
  }
}

abstract class NetworkBuilder(val numNodes: Int) extends Actor {
  val nodes = Array.fill(numNodes)(new Node)
  val rand = new Random(System.currentTimeMillis())
  def randomNode: Node = nodes(rand.nextInt(nodes.length))
  def neighbors(x: Int): Array[Node]    // implemented by different topologies
  def act() {
    nodes foreach(_.start)
    Array.range(0, numNodes) foreach (x => (nodes(x) ! (x,neighbors(x))))
    randomNode ! "start"
  }
}

class Full(numNodes: Int) extends NetworkBuilder(numNodes) {
  def neighbors(x: Int): Array[Node] =
    Array.range(0, numNodes) filter (_!=x) map (nodes(_))
}

class Line(numNodes: Int) extends NetworkBuilder(numNodes) {
  private def inRange(x: Int): Boolean = x>=0 && x<numNodes
  def neighbors(x: Int): Array[Node] =
    Array(x-1, x+1) filter (inRange(_)) map (nodes(_))
}

class Grid(val rows: Int, val cols: Int) extends NetworkBuilder(rows*cols) {
  private def at(r: Int, c: Int): Node = nodes(r*cols+c)
  private def inRange(r: Int, c: Int): Boolean = r>=0 && c>=0 && r<rows && c<cols
  private def neighbors(r: Int, c: Int): Array[Node] =
    (Array(r-1,r,r+1,r), Array(c,c-1,c,c+1)).zipped.filter(inRange(_,_)).zipped.map(at(_,_))
  def neighbors(x: Int): Array[Node] = neighbors(x/cols, x%cols)
}

class ImperfectGrid(rows: Int, cols: Int) extends Grid(rows, cols) {
  override def neighbors(x: Int): Array[Node] = super.neighbors(x) :+ randomNode
}

object project2 extends App {
  new Grid(10,10) start
}
