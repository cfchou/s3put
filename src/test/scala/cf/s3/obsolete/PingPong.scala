package cf.s3.obsolete

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}
import scala.concurrent.{ExecutionContext, Await, Future}

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 03/02/2014
 */

object PingPong extends App {
  val system = ActorSystem("PingPong")

  val pong = system.actorOf(Props(new Pong), "pong")
  val ping = system.actorOf(Props(new Ping(pong)), "ping")

  import ExecutionContext.Implicits.global
  system.scheduler.scheduleOnce(10.seconds)({
    println("Shutdown actors")
    system.stop(ping)
    system.stop(pong)
    system.shutdown()
  })

}

class Ping(pong: ActorRef) extends Actor with ActorLogging {

  import Pong._

  implicit val ec = context.system.dispatcher

  /* Ask's timeout. As long as result from Pong comes back before the timeout,
   * the temp actor in ask pattern is valid.
   */
  val duration = Pong.scheduleDelay + (1 second)
  implicit def timeout = Timeout(duration)

  log.info(s"ping start ${self.path}")

  (pong ? "000") onComplete({
    case Success(v) => log.info(s"000 $v")
    case Failure(e) => log.info(s"000 $e")
  })

  (pong ? "111") onComplete({
    case Success(v) => log.info(s"111 $v")
    case Failure(e) => log.info(s"111 $e")
  })


  //
  (pong ? "222") transform({ x =>
    log.info(s"222 $x")
    x
  }, { e =>
    log.info(s"222 $e")
    e
  })

  (pong ? GetBack("333")).transform({ x =>
    log.info(s"--> GetBack $x")
    x
  }, { e =>
    log.info(s"--> GetBack $e")
    e
  })

  def receive: Receive = {
    case x =>
      log.info(s"Ping: $x")
  }
}

object Pong {
  implicit val scheduleDelay = 1 second
  case class GetBack(msg: String)
  case class GetBackTo(commander: ActorRef, msg: String)
}

class Pong extends Actor with ActorLogging {

  import Pong._

  case class ResponseGetBack(commander: ActorRef, msg: String)

  implicit val ec = context.system.dispatcher

  implicit val timeout = Timeout(scheduleDelay)

  log.info(s"pong start ${self.path}")
  def receive: Receive = {

    case x@"000" =>
      log.info(s"Pong: $x from ${sender.path}")
      sender ! x
    case x@"111" =>
      log.info(s"Pong: $x from ${sender.path}")
      /* Rule of thumb: "val commander = sender" and then use commander
       * instead of sender in the closure of Future
       */
      val commander = sender
      Await.result(Future {
        commander ! x
      }, 1.second)

    case x@"222" =>
      log.info(s"Pong: $x from ${sender.path}")
      val commander = sender
      Future {
        // if client "asks", then the sender is a temp actor provided by akka.
        commander ! x
      }

    // -------
    case x@GetBack(msg) =>
      log.info(s"Pong: $x from ${sender.path}")
      val commander = sender

      context.system.scheduler.scheduleOnce(scheduleDelay)({
        // Use sender as commander is not reliable, since it could be a temp
        // actor in the case that client's using ask pattern
        self ! ResponseGetBack(commander, msg)
      })
    case x@GetBackTo(commander, msg) =>
      log.info(s"Pong: $x from ${sender.path}")
      context.system.scheduler.scheduleOnce(scheduleDelay)({
        self ! ResponseGetBack(commander, msg)
      })
    case x@ResponseGetBack(commander, msg) =>
      log.info(s"Pong: $x from ${sender.path}")
      commander ! GetBack(msg)
    // -------

    case x => log.info(s"Pong: $x")
  }
}