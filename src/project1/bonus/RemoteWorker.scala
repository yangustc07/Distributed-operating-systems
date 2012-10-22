package project1
package bonus

import scala.math._
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

class RemoteWorker extends Actor {
  def act() {
    react {
      /* b, e indicate the range of subproblem this actor is supposed to solve.
       * b - beginning index;  e - end index
       * an = a1 + k(n-1)(n+k) */
      case (a1: Long, b: Long, e: Long, k: Long, boss: AbstractActor) =>
        for (i <- b until e) {
          val ai = a1 + (i-1)*(i+k)*k
          val root = round(sqrt(ai))
          if(ai == root*root)
            boss ! (root toLong, i toLong) // finds a solution
        }
        boss ! 1l // indicating finish
    }
  }
}

class RemoteBoss extends Actor {
  private val nparts = 4
  def act() {
    alive(9010)
    register('remoteboss, self)
    loop {
      react {
        case (b: Long, e: Long, k: Long) =>
          val boss = select(Node("lin114-10.cise.ufl.edu", 9000), 'boss)
          for (j <- 1 to nparts) {
      	    val a1 = k*(k+1)*(2*k+1)/6
            val worker = new RemoteWorker
            worker.start
            worker ! (a1, b+(j-1)*(e-b+1)/nparts, b+j*(e-b+1)/nparts, k, boss)
          }
      }
    }
  }
}

object RemoteWorker extends App {
  (new RemoteBoss).start
}
