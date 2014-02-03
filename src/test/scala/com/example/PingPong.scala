package com.example

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.concurrent.{Await, Future}

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 03/02/2014
 */

object PingPong extends App {

  val system = ActorSystem("AskTest")

  val pong = system.actorOf(Props(new Pong), "pong")
  val ping = system.actorOf(Props(new Ping(pong)), "ping")


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
  }
}