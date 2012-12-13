package project4

import util.Random

import scala.actors._
import scala.actors.Actor._

import scala.actors._
import scala.actors.Actor._
import scala.math.abs
import scala.util.Random

sealed trait Message
case object Looping extends Message
case object Rumor extends Message
case object BadRumor extends Message
case object Stop extends Message
case object Remove extends Message   // when a neighbor exits, it must be removed from the neighbor list

case class Alarm(i: Int) extends Message

abstract class Node extends Actor with Logging {
  val rand = new Random
  var count: Int = 0
  var neighbors: Array[Node] = Array.empty
  var boss: NetworkBuilder = null
  def randomNeighbor: Node = 
    if (neighbors isEmpty) this  // in case all neighbors exited
    else neighbors(rand.nextInt(neighbors.length))
  def init(n: Array[Node], b: NetworkBuilder) = {
    neighbors = n; boss = b;
    rand.setSeed(System.currentTimeMillis() ^ (neighbors.hashCode toLong))
  }
  override def exit() = {
    neighbors foreach(_ ! (Remove, self))
    super.exit()
  }
  def removeNeighbor(a: Node) = (neighbors = neighbors filter(_.id != a.id))
  override def toString = id.toString
}

class GossipNode extends Node {
  private def rep[A](n: Int)(f: => A) { if (n > 0) { f; rep(n-1)(f) } }
  private val maxCount = 20
  private def sendLoopMessage() = { self ! Looping }
  private def spreadMessage() =
    if(count>0) randomNeighbor ! Rumor
  var badCount = 0
  def act() {
    loopWhile(count<maxCount) {
      sendLoopMessage()
      spreadMessage()
      react {
        case (n: Array[Node], b: NetworkBuilder) => init(n, b)
        case Rumor => count += 1
        case BadRumor => 
          badCount += 1
          if (badCount > 1) { boss ! Alarm(id) }
          else { rep(2)(randomNeighbor ! BadRumor) }
        case Looping => ;
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
    Array.range(0, numNodes) foreach (x => (nodes(x) ! (neighbors(x),self)))
    nodes.head ! Rumor
    nodes(70) ! BadRumor
    val b = System.currentTimeMillis
    loop {
      react {
        case Exit(_,_) => count+=1
          if(count>=numNodes) {
            println("Time = "+(System.currentTimeMillis-b)+" ms")
            exit()
          }
        case Alarm(id) =>
          new Detector(id) trace;
          nodes foreach (_ ! Stop); exit()
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
  private val groups = Random.shuffle(List.range(0, numNodes)).splitAt(numNodes/2)
  private val partnerMap = (groups._1 zip groups._2).toMap ++ (groups._2 zip groups._1).toMap
  private def partner(x: Int) = if (partnerMap contains x) partnerMap(x) else x
  override def neighbors(x: Int): Array[Node] = super.neighbors(x) :+ nodes(partner(x))
}

object Gossip {
  def main(args: Array[String]) {
    val network = new Full(100) start;
  }
}
