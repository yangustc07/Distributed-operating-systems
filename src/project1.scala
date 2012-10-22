import scala.math._
import scala.actors._
import scala.actors.Actor._

class WorkerActor extends Actor {
  def act() {
    react {
      /* b, l indicate the range of subproblem this actor is supposed to solve.
       * b - beginning index; l - length of the range
       * an = a1 + k(n-1)(n+k) */
      case (a1: Long, b: Long, l: Long, k: Long, boss: Actor) =>
        for (i <- b to (b+l-1)) {
          val ai = a1 + (i-1)*(i+k)*k
          val root = round(sqrt(ai))
          if(ai == root*root)
            boss ! (root, i) // finds a solution
        }
        boss ! 1l // indicating finish
    }
  }
}

class BossActor(N: Long, k: Long, np: Int) extends Actor {
  private val a1 = k*(k+1)*(2*k+1)/6
  private val nparts = np  // # of subproblems
  private def format(root: Long, beg: Long): String = // formatting output string
    ""+root+"^2 = "+(for(i <- beg to (beg+k-1)) yield ""+i+"^2").mkString("+")

  def act() {
    var done: Int = 0
    for (j <- 1 to nparts) { // initializing workers and assign subtasks
      val worker = new WorkerActor
      worker.start
      worker ! (a1, 1+(j-1)*N/nparts ,N/nparts, k, self)
    }
    loop {
      react {
        case (root: Long, beg: Long) =>  // receive a solution
          println(format(root,beg))
        case (fin: Long) =>   // record the number of finished workers
          done += 1
          if (done >= nparts)
            exit()
      }
    } 
  }
}

object project1 {
  def main(args: Array[String]) {
    val N = if (args.length > 0) args(0) toInt else 1000000l  // size of problem
    val k = if (args.length > 1) args(1) toInt else 2l        // length of sum sequence
    val np = if (args.length > 2) args(2) toInt else 4        // # of subproblems (partitions)
    val w = new BossActor(N,k,np)
    w.start
  }
}
