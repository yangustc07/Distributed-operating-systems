package project3

import scala.collection.immutable.BitSet
import scala.util.Random

import scala.actors._
import scala.actors.Actor._

sealed trait Message
case object Join extends Message
case object LeafSet extends Message
case object RoutingTable extends Message
case object NeighborhoodSet extends Message
case object Closest extends Message

class NodeID(val bits: Long) extends Ordered[NodeID] {  // 64 bits
  val (b, n) = (2, 64)   // n = sizeof(Long)
  def digit(i: Int): Int = ((bits>>(b*i)) & ((1l<<b)-1)) toInt
  private def groupZeros(x: Long, grouped: Int): Int =
    if ((x&(1l<<b-1)) >0 || grouped > n/b-1) grouped else groupZeros(x>>b, grouped+1)
  def matched(key: NodeID): Int = groupZeros(bits ^ key.bits, 0)
  def sameID(that: NodeID) = this.bits == that.bits
  def distance(that: NodeID) = this.bits - that.bits
  override def compare(that: NodeID): Int = 
    if (this.bits > that.bits) 1 else if (this.bits < that.bits) -1 else 0
  override def toString() = "%016x".format(bits)
}

object NodeID {
  val rand = new Random
  def zero: NodeID = 0l
  def randomId: NodeID = rand.nextLong()
  implicit def long2id(x: Long): NodeID = new NodeID(x)
}

class PastryNode extends Actor with Ordered[PastryNode] {
  val nid = NodeID.randomId
  val (b, l) = (2, 8)  // configuration constants
  val (rows, cols) = (128/b, 1<<b)
  private def at(r: Int, c: Int) = routingTable(r*cols + c)
  override def compare(that: PastryNode) = this.nid.compare(that.nid)
  def min(that: PastryNode) = if (this<that) this else that  // why ordered trait doesn't support min/max?
  def max(that: PastryNode) = if (that<this) this else that
  def distance(that: PastryNode) = this.nid.distance(that.nid)
  val routingTable: Array[PastryNode] = Array.fill(rows*cols)(null)
  val smaller, larger: Array[PastryNode] = Array.fill(l/2)(null)
  def leafSet = smaller ++ larger  // TODO: check for null values
  def leafSet_=(ls: Array[PastryNode]) {
    ls.filter(_<this).copyToArray(smaller)
    ls.filterNot(_<this).copyToArray(larger)
  }
  def route(node: PastryNode): PastryNode = {
    def closest(nodes: Array[PastryNode]) =
      nodes.reduceLeft { (x,y) => if (x.distance(node) < y.distance(node)) x else y }
    val (minLeaf, maxLeaf) = (smaller.reduceLeft(_ min _), larger.reduceLeft(_ max _))
    if (minLeaf <= node && node <= maxLeaf) closest(leafSet)
    else {  // using the routing table
      val shl = nid matched node.nid  // length of shared prefix in digits
      if (at(shl, node.nid.digit(shl)) != null) at(shl, node.nid.digit(shl)) // use routing table
      else closest(leafSet ++ routingTable.drop(shl*cols)) // rare case -> use numerical closer
    }
  } ensuring (_.distance(node) < this.distance(node))
  def act = {
    PastryBoss ! (Join, self)
    loop {
      react {
        // received a join request from a new node. Route to Z and send this node state table to PastryNode
        case (Join, newComer: PastryNode) => 
          if (newComer == self) println("self join")
          else {
            val rowid = nid.matched(newComer.nid)
            newComer ! (RoutingTable, routingTable.slice(rowid*cols, rowid*(cols+1)), rowid)
            val nextNode = route(newComer)
            if (nextNode == self) {  // I'm the Z node (closest)
              newComer ! (Closest, (nid, self), leafSet)
            } else {  // needs next route
              nextNode ! (Join, newComer)
            }
          }
      //if you are nodeId -1, send your neighborhood set
	  //if you are closest to nodeId, send your leaf set
	  //update state tables
        case(RoutingTable, rt: Array[PastryNode], rowid: Int) => 
          Array.copy(rt, 0, routingTable, rowid*cols, cols)  // this may cause errors, general case only
        case(Closest, z: PastryNode, ls: Array[PastryNode]) =>
          leafSet = ls
          // send its own state to all the nodes in its state tables
          routingTable.foreach(_ ! routingTable)
          routingTable.foreach(_ ! leafSet)
          smaller.foreach(_ ! routingTable)
          smaller.foreach(_ ! leafSet)
          larger.foreach(_ ! routingTable)
          larger.foreach(_ ! leafSet)
      }
    }
  }
}

object PastryBoss extends PastryNode {
  
}

object Node extends App {
  val id = new NodeID(7)
  println(id)
  println(id.digit(0))
  println(id.digit(1))
  println(id.digit(2))
  println(id.digit(3))
}