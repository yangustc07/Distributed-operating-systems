import scala.actors._
import scala.math

class SillyActor(val id:Int) extends Actor {
       def act() {
       	   react {      
	   	 case(start: Int, end: Int, k: Int) =>
		 	 for(i <- start to end){
			  var sum: Int = 0
			  sum = k * i * i + (k * (k-1)* (2*k - 1))/6 + i * k * (k-1)
			  if (math.sqrt(sum) == math.rint(math.sqrt(sum)))
			  println(i)
			 }
			 exit()
	   }      
	 }
}


object Boss extends Actor{
       def act {
       	   react {
	   	 case(n: Int, k: Int, wu: Int) =>
		 	 for(i <- 1 to n by wu) {
			       val a = new SillyActor(i)
			       a.start()
			      /** println("Starting Actor" + (i, i+wu-1, k))*/
			       a ! (i, i+wu-1, k)
			 }
			 exit()
	    }
	}
}

object project1 {
  def main(args: Array[String]) {
		val wu = 10000000
		val n: Int = args(0).toInt
		val k: Int = args(1).toInt
    Boss.start()
    Boss ! (n, k, wu)
  }
}

/**
println(args(0) + " " + args(1))
Boss ! (args(0), args(1))
*/