package project4

import scala.actors._
import scala.actors.Actor._

class Example extends Actor with Logging {
  import Example._
  val b = System.currentTimeMillis
  def act() = {
    //loopWhile(System.currentTimeMillis-b < 100) {
      react { case x: Int => randomActor ! (x+1) }
    //}
  }
}

object Example {
  val rand = new util.Random
  val egs = Vector.fill(10)(new Example)
  def randomActor = rand.shuffle(egs).head
  def main(args: Array[String]) {
    egs foreach (_.start)
    egs foreach (_ ! 5)
  }
}