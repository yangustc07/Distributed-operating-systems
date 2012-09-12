import scala.math._
import scala.actors._
import scala.actors.Actor._
import scala.actors.remote._
import scala.actors.remote.RemoteActor._

class WorkerActor extends Actor {
  def act() {
    alive(9010)
    register('worker, self)
    react {
      /* b, l indicate the range of subproblem this actor is supposed to solve.
       * b - beginning index; l - length of the range
       * an = a1 + k(n-1)(n+k) */
      case (a1: Long, b: Long, l: Long, k: Long) =>
        val boss = select(Node("lin114-10.cise.ufl.edu", 9000), 'boss)
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

object WorkerActor extends App {
  (new WorkerActor).start
}
