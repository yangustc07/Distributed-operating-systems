import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

class Boss(N: Long, k: Long, np: Int) extends Actor {
  require(np<=10)
  private val nparts = np  // # of subproblems
  private def format(root: Long, beg: Long): String = // formatting output string
    ""+root+"^2 = "+(for(i <- beg to (beg+k-1)) yield ""+i+"^2").mkString("+")

  def act() {
    alive(9000)
    register('boss, self)
    var done: Int = 0
    for (j <- 1 to nparts) {
      val remoteBoss = select(Node("lin114-"+"%02d".format(j-1)+".cise.ufl.edu", 9010), 'remoteboss)
      remoteBoss ! (1+(j-1)*N/nparts, 1+j*N/nparts, k)
    }
    loop {
      react {
        case (root: Long, beg: Long) => // receive a solution
          println(format(root,beg))
        case (fin: Long) => // record the number of finished workers
          done += 1
          if (done >= nparts)
            exit()
      }
    }
  }
}

object BossActor {
  def main(args: Array[String]) {
    val N = if (args.length > 0) args(0) toInt else 1000000l  // size of problem
    val k = if (args.length > 1) args(1) toInt else 2l        // length of sum sequence
    val np = if (args.length > 2) args(2) toInt else 4        // # of subproblems (partitions)
    val w = new Boss(N,k,np)
    w.start
  }
}
