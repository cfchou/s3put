package com.example

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
  implicit val ec = context.system.dispatcher
  implicit val timeout = Timeout(1 seconds)

  log.info(s"ping start ${self.path}")
  (pong ? "000") onComplete({
    case Success(v) => log.info(s"000 $v")
    case Failure(e) => log.info(s"000 $e")
  })

  (pong ? "111") onComplete({
    case Success(v) => log.info(s"111 $v")
    case Failure(e) => log.info(s"111 $e")
  })

  (pong ? "222") transform({ x =>
    log.info(s"222 $x")
    x
  }, { e =>
    log.info(s"222 $e")
    e
  })

  def receive: Receive = {
    case x =>
      log.info(s"Ping: $x")
  }
}

class Pong extends Actor with ActorLogging {
  implicit val ec = context.system.dispatcher
  implicit val timeout = Timeout(1 seconds)

  log.info(s"pong start ${self.path}")
  def receive: Receive = {
    case x@"000" =>
      log.info(s"Pong: $x to ${sender.path}")
      sender ! x
    case x@"111" =>
      log.info(s"Pong: $x to ${sender.path}")
      Await.result(Future {
        sender ! x
      }, 1.second)
    case x@"222" =>
      log.info(s"Pong: $x to ${sender.path}")
      Future {
        // if client "asks", then the sender is a temp actor provided by akka.
        // therefore msg won't go to client.
        // So the Future need to redeem by Await, thus the temp actor remains valid
        // on this thread, like case "111".
        sender ! x
      }
    case x => log.info(s"Pong: $x")
  }
}