import scala.actors._
import scala.actors.Actor._
import scala.math.abs
import scala.util.Random

sealed trait Message
case object Looping extends Message
case object Rumor extends Message
case object Stop extends Message
case object Remove extends Message   // when a neighbor exits, it must be removed from the neighbor list
case class Sum(s: Double) extends Message
case class PartialSum(val s: Double, val w: Double) extends Message

abstract class Node extends Actor {
  var id: Int = 0
  val rand = new Random
  var count: Int = 0
  var neighbors: Array[Node] = Array.empty
  var boss: NetworkBuilder = null
  def randomNeighbor: Node = 
    if (neighbors isEmpty) this  // in case all neighbors exited
    else neighbors(rand.nextInt(neighbors.length))
  def init(i: Int, n: Array[Node], b: NetworkBuilder) = {
    id = i; neighbors = n; boss = b;
    rand.setSeed(System.currentTimeMillis() ^ (neighbors.hashCode toLong))
  }
  override def exit() = {
    neighbors foreach(_ ! (Remove, self))
    super.exit()
  }
  def removeNeighbor(a: Node) {
    neighbors = neighbors filter(_.id != a.id)
  }
}

class GossipNode extends Node {
  private val maxCount = 10
  private def sendLoopMessage() = { self ! Looping }
  private def spreadMessage() =
    if(count>0) randomNeighbor ! Rumor
  def act() {
    loopWhile(count<maxCount) {
      sendLoopMessage()
      spreadMessage()
      react {
        case (i: Int, n: Array[Node], b: NetworkBuilder) => init(i, n, b)
        case Rumor => count += 1
          if (count==maxCount)println(id+": "+count)
        case Looping => sendLoopMessage()
        case Stop => exit()
        case (Remove, a: Node) => removeNeighbor(a)
      }
    }
  }
}

class PushSumNode extends Node {
  private var s: Double = 0
  private var w: Double = 1
  private val nConv: Int = 5    // n convergence tests
  private val converging: Array[Boolean] = Array.fill(nConv)(false)
  override def init(i: Int, n: Array[Node], b: NetworkBuilder) =
    { super.init(i, n, b); s = i }
  private def spreadMessage() = {
    randomNeighbor ! PartialSum(s/2.0, w/2.0)
    s /= 2.0; w /= 2.0
  }
  def act() {
    loop {
      react {
        case (i: Int, n: Array[Node], b: NetworkBuilder) => init(i, n, b)
        case Rumor => spreadMessage()
        case PartialSum(ss, ww) => count += 1
          converging(count%nConv) = abs(s/w-(s+ss)/(w+ww)) < 1e-10
          s += ss; w += ww; 
          if(converging forall(_ == true)) exit(Sum(s/w))
          else spreadMessage()
        case Stop => exit()
        case (Remove, a: Node) => removeNeighbor(a)
      }
    }
  }
}

abstract class NetworkBuilder(val numNodes: Int) extends Actor {
  private var count = 0
  val nodes = Array.fill(numNodes)(new GossipNode)
  val rand = new Random(System.currentTimeMillis())
  def randomNode: Node = nodes(rand.nextInt(nodes.length))
  def neighbors(x: Int): Array[Node]    // implemented by different topologies
  def act() {
    trapExit = true
    nodes foreach(x => {link(x); x start})
    Array.range(0, numNodes) foreach (x => (nodes(x) ! (x,neighbors(x),self)))
    randomNode ! Rumor
    val b = System.currentTimeMillis
    loop {
      react {
        case Exit(_,Sum(s)) =>
          println("sum = "+(numNodes*s)+"\nTime = "+(System.currentTimeMillis-b)+" ms.")
          nodes foreach(_ ! Stop)
          exit()
        case Exit(_,_) => count+=1
        //println("count = "+count)
          if(count>=numNodes) {
            println((System.currentTimeMillis-b)+" ms")
            exit()
          }
      }
    }
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
  val swapper = Random.shuffle(List.range(0, numNodes)) toArray   // match
  def partner(x: Int) = nodes(swapper(x%2 match {
    case 0 if (x+1<numNodes) => x+1    // even => match next
    case 1 => x-1                      // odd => match previous
    case _ => x                        // no match
  }))
  override def neighbors(x: Int): Array[Node] = super.neighbors(x) :+ partner(x)
}

object project2 {
  def main(args: Array[String]) {
    (new ImperfectGrid(10,10)).start
    //println(Array.range(0, 100).reduceLeft(_+_))
  }
}
