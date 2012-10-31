package project3

import scala.util.Random

import scala.actors._
import scala.actors.Actor._

sealed trait Message
case object Join extends Message
case object Joined extends Message
case object LeafSet extends Message
case object Register extends Message
case object RouteObject extends Message
case object RoutingTable extends Message
case object Update extends Message
case object NeighborhoodSet extends Message
case object Closest extends Message
case object Exit extends Message
case class Nhops(n: Int) extends Message

class NodeID(val bits: Long) extends Ordered[NodeID] {  // 64 bits
  val (b, n) = (2, 64)   // n = sizeof(Long)
  def digit(i: Int): Int = ((bits>>(b*i)) & ((1l<<b)-1)) toInt
  private def groupZeros(x: Long, grouped: Int): Int =
    if ((x&((1l<<b)-1)) >0 || grouped > n/b-1) grouped else groupZeros(x>>b, grouped+1)
  def matched(key: NodeID): Int = groupZeros(bits ^ key.bits, 0) ensuring (_ < n/b)
  def sameID(that: NodeID) = this.bits == that.bits
  private def matchedBits(x: Long, acc: Int): Int =
    if ((x&1l)>0 || acc>=n) acc else matchedBits(x>>1, acc+1)
  def distance(that: NodeID) = n - matchedBits(bits ^ that.bits, 0)
  override def compare(that: NodeID): Int = 
    if (this.bits > that.bits) 1 else if (this.bits < that.bits) -1 else 0
  override def toString() = "%016x".format(bits)
}

object NodeID {
  val rand = new Random
  def zero: NodeID = 0l
  def randomId: NodeID = rand.nextLong()
  implicit def long2id(x: Long): NodeID = new NodeID(x)
  implicit def node2id(x: PastryNode): NodeID = x.nid
}

class PastryNode extends Actor with Ordered[PastryNode] with Ordering[PastryNode] {
  val nid = NodeID.randomId
  val (b, l) = (2, 8)  // configuration constants
  val (rows, cols) = (64/b, 1<<b)
  private def at(r: Int, c: Int) = routingTable(r*cols + c)
  override def compare(that: PastryNode) = this.nid.compare(that.nid)
  override def compare(n1: PastryNode, n2: PastryNode) = n1.compare(n2)
  def distance(that: PastryNode) = this.nid.distance(that.nid)
  val routingTable: Array[PastryNode] = Array.fill(rows*cols)(Nil)
  val smaller, larger: Array[PastryNode] = Array.fill(l/2)(Nil)
  def leafSet = smaller ++ larger  // TODO: check for Nil values
  def leafSet_=(ls: Array[PastryNode]) {
    ls.filter(_<this).copyToArray(smaller)
    ls.filterNot(_<this).copyToArray(larger)
  }
  def insertLeafSet(x: PastryNode) {
    def insert(s: Array[PastryNode])(cmp: (PastryNode, PastryNode)=> Boolean) { // s is either smaller or larger
      val idxNil = s.indexOf(Nil)  // replace null's first
      if(idxNil >= 0) s(idxNil) = x
      else {  // replace the smallest/largest
        val idx = s.indices.reduceLeft { (x,y) => if (cmp(s(x), s(y))) x else y }
        val extremal = s(idx)
        if (cmp(extremal, x)) s(idx) = x
      }
    }
    if (x<this) insert(smaller)(_ < _) else insert(larger)(_ > _)
  }
  def insertRoutingTable(x: PastryNode) {
    val shl = nid.matched(x.nid)
    val col = x.nid.digit(shl)
    if (at(shl,col)==Nil) routingTable(shl*cols+col)=x
  }
  def insertNode(x: PastryNode) { insertLeafSet(x); insertRoutingTable(x) }
  def route(id: NodeID): (PastryNode, Boolean) = {  // TODO: Very hard to check Nil values!!!
    def closest(nodes: Array[PastryNode]) = // the Boolean indicates whether this is the final route
      if (nodes.forall(_==Nil)) Nil
      else nodes.filter(_!=Nil).reduceLeft { (x,y) => if (x.distance(id) < y.distance(id)) x else y }
    val leafs = leafSet.filter(_!=Nil)
    val (minLeaf, maxLeaf) =
      if (leafs.isEmpty) (Nil,Nil)
      else (leafs.reduceLeft(_ min _), leafs.reduceLeft(_ max _))
    if (!leafSet.isEmpty && minLeaf <= id && id <= maxLeaf) (closest(leafSet), true)
    else {  // using the routing table
      val shl = nid matched id  // length of shared prefix in digits
      if (at(shl, id.digit(shl)) != Nil) (at(shl, id.digit(shl)), false) // use routing table
      //else (closest(leafs ++ routingTable.drop(shl*cols)), false) // rare case -> use numerical closer
      else {
        val closer = closest(routingTable.drop(shl*cols))
        if(closer.nid.distance(id)>=this.nid.distance(id)) (Nil,false) else (closer,false)
      }
    }
  }
  def routeNext[T <% NodeID](id: T, hasRows: Int, n: Int, closest: Boolean)(msgobj: Message)(FinalAction: => Unit) {
    if (closest) FinalAction
    else {
      val matchedRows = nid.matched(id)
      id match {
        case newComer: PastryNode =>
          newComer ! (RoutingTable, routingTable.slice(hasRows*cols, (matchedRows+1)*cols), hasRows*cols, (matchedRows+1)*cols)
        case _ => ;  // for objects, don't have to send routing table entries
      }
      val (nextNode, close) = route(id)
      assert (close || nextNode==self || nextNode==Nil || nextNode.nid.distance(id)<this.nid.distance(id))
      if (nextNode==self || nextNode==Nil) FinalAction
      else nextNode ! (msgobj, id, matchedRows, n+1, close)
    }
  }
  protected val work: PartialFunction[Any, Unit] = {
    // received a join request from a new node. Route to Z and send this node state table to PastryNode
    case (Join, newComer: PastryNode, hasRows: Int, n: Int, closest: Boolean) => 
      routeNext(newComer, hasRows, n, closest)(Join){ newComer ! (Closest, self); PastryBoss ! Joined }
    case (RouteObject, id: NodeID, hasRows: Int, n: Int, closest: Boolean) =>
      routeNext(id, hasRows, n, closest)(RouteObject){ PastryBoss ! Nhops(n) }
    //if you are closest to nodeId, send your leaf set
    //update state tables
    case (RoutingTable, rt: Array[PastryNode], start: Int, end: Int) => assert(start<=end)
      Array.copy(rt, 0, routingTable, start, end-start)  // this may cause errors, general case only
    case (Closest, z: PastryNode) => assert(z!=Nil)
      leafSet = z.leafSet
      z ! (Update, self)
      // send its own state to all the nodes in its state tables
      Array(routingTable, smaller, larger) foreach {
        table => table foreach (_ ! (Update, self)) }
    case (Update, x: PastryNode) => assert(x!=Nil) // update state tables
      insertNode(x)
    case Exit => exit()
  }
  protected val bossWork: PartialFunction[Any, Unit] = Map.empty
  def act = {
    PastryBoss ! (Join, self, 0, 0, false)
    PastryBoss ! (Register, self)
    loop(react(work orElse bossWork))
  }
}

object Nil extends PastryNode
object PastryBoss extends PastryNode {
  var count, sum, numNodes, numRequests = 0
  var participants: Array[PastryNode] = Array.empty
  def init(nn: Int, nr: Int) { numNodes = nn; numRequests = nr }
  def genRequest() = randomNode ! (RouteObject, NodeID.randomId, 0, 0, false)
  def randomNode = participants(NodeID.rand.nextInt(participants.length))
  override val bossWork: PartialFunction[Any, Unit] = {
    case (Register, x: PastryNode) => participants :+= x
    case Joined => count += 1
      if (count>=numNodes-1) { // all participants have joined ==> begin file protocal
        count = 0
        //println("The network is ready.") // TODO: for debugging. remove this println later
        Array.range(0, numRequests).foreach(x=>genRequest)
      }
    case Nhops(n) => count += 1; sum += n
      if (count>=numRequests) {
        println(sum.toDouble/numNodes.toDouble)
        participants.foreach(_ ! Exit)
        exit
      }
  }
  override def act = loop(react(work orElse bossWork))
}

object Node {
  def main(args: Array[String]) {
    val (numNodes, numRequests) = try {
      (args(0).toInt, args(1).toInt)
    } catch {
      case _ => println("Warning: using default configuration (1000,1000)")
      (1000, 1000)
    }
    PastryBoss.init(numNodes, numRequests)
    PastryBoss.start
    Array.fill(numNodes)(new PastryNode) foreach(_ start)
  }
}