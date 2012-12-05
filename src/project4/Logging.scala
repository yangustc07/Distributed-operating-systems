package project4

import scala.actors._
import scala.actors.Actor._

import java.io._

@serializable final case class withClock(msg: Any, id: Int, clock: Int)

trait Logging extends Actor {
  val id = Logging.inc   // used to identify actors in log
  val logfileName = id +".out"
  writeToFile(logfileName, "", false)  // erase old logs
  
  // utility methods used to maintain logs and clocks
  private def getOrElse[T](get: Logging => T, default: T): T =
    self match { case x: Logging => get(x) case _ => default }
  private def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }
  private def writeToFile(filename: String, data: String, append: Boolean = true) =
    using (new FileWriter(filename, append)) (_.write(data))
  private def writeLog(data: String) = getOrElse(x => writeToFile(x.logfileName, data), ())
  private def senderId = getOrElse(_.id, 0)
  private def senderClock = getOrElse(_.lamportClock, 0)

  private var lamportClock = 0
  private def advanceClock(x: Int) { lamportClock = (lamportClock max x) +1 }
  private def advanceSenderClock() { getOrElse(x => x.lamportClock+=1, ()) }
  
  override def react(handler: PartialFunction[Any, Unit]) = {
    super.react {
      case withClock(msg: Any, fromId: Int, fromClock: Int) =>   // unwrap
        advanceClock(fromClock)
        writeLog("At [" +lamportClock+ "]: (" +id+ ") recieved message from (" +fromId+ "): " +msg+ "\n")
        handler.apply(msg)
      case _ => throw new RuntimeException   // should not happen since ! is overridden
    }
  }

  override def !(msg: Any) = {
    advanceSenderClock()
    val (fromId, fromClock) = (senderId, senderClock)
    writeLog("At [" +fromClock+ "]: (" +fromId+ ") sent message to (" +id+ "): " +msg+ "\n")
    super.!(withClock(msg, fromId, fromClock))   // wrap message with sender id and clock
  }
}

object Logging {
  private var idx = 0
  private def inc = { idx+=1; idx }   // used to generate actor IDs
}