package project3

import scala.collection.immutable.BitSet

import scala.actors._
import scala.actors.Actor._

class NodeID(mostSigBits: Long, leastSigBits: Long) {  // 128 bits
  val b = 2
  val bits = BitSet.fromArray(Array[Long](leastSigBits, mostSigBits))
  def digit(i: Int): Int = bits.filter(x => (b*i<=x && x<b*(i+1))).map(_-b*i)
  def matched(key: NodeID): Int = {
    val xor = bits ^ key.bits
    if (xor isEmpty) 128/b else xor.head/2
  }
  implicit def bits2int(x: BitSet): Int = x.foldLeft(0)((x,y)=>x+(1<<y))
  override def toString() = "%016x".format(mostSigBits) +"-"+ "%016x".format(leastSigBits)
}

object NodeID {
  import java.util.UUID
  def randomId: NodeID = UUID.randomUUID
  implicit def uuid2nodeid(x: UUID): NodeID = new NodeID(x.getMostSignificantBits, x.getLeastSignificantBits)
}

class Node extends Actor {
  val nid = NodeID.randomId
  val (b, l) = (2, 8)  // configuration constants
  val (rows, cols) = (128/b, 1<<b)
  private def at(r: Int, c: Int) = routingTable(r*cols + c)
  val routingTable: Array[Int] = new Array(rows*cols)
  object leafSet {
    val smaller, larger = new Array[NodeID](l/2)
  }
  def act = {}
}

object Node extends App {
  val (nid1, nid2) = (new NodeID(1,1), new NodeID(2,1))
  println(nid1.bits)
  println(nid2.bits)
  println(nid1.bits ^ nid2.bits)
  println(nid1)
  println(nid2)
  println(nid1 matched nid2)
}