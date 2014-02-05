package com.example

import akka.actor._
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}
import scala.concurrent.{ExecutionContext, Await, Future}
/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 05/02/2014
 */


object FSMoo extends App {
  val system = ActorSystem("FSMoo")

  val moo = system.actorOf(Props(new Mood), "mood")
  val jo = system.actorOf(Props(new Person(moo)), "Jo")

  import ExecutionContext.Implicits.global
  system.scheduler.scheduleOnce(10.seconds)({
    println("Shutdown actors")
    system.stop(jo)
    system.stop(moo)
    system.shutdown()
  })

}

object Person {
  trait Emo
  case object Happy extends Emo
  case object Upset extends Emo
  case object Neutral extends Emo
  case object NeverFeelThisWay extends Emo

  type Content = List[String]

  trait News
  case object Nothing extends News
  case class Info(data: String) extends News
  case class GoodNews(data: String) extends News
  case class BadNews(data: String) extends News
}

class Person(mood: ActorRef) extends Actor with ActorLogging {
  import Person._

  val system = context.system
  implicit val ec = system.dispatcher
  implicit val timeout = Timeout(1 second)

  mood ! Nothing
  mood ! Info("today is wednesday")
  mood ! Nothing
  mood ! GoodNews("sunny day")
  mood ! Nothing
  mood ! "@#$%^..."
  system.scheduler.scheduleOnce(3 second) {
    mood ! GoodNews("good food")
  }

  def receive = {
    case x => log.info(s"$x")
  }
}

class Mood extends Actor with FSM[Person.Emo, Person.Content]
  with ActorLogging {
  import Person._

  startWith(Neutral, List.empty)
  when(Neutral) {
    case Event(x@Nothing, ct) =>
      log.info(s"${sender.path} sends $x, $ct")
      stay
    case Event(x@Info(i), ct) =>
      log.info(s"${sender.path} sends $x, $ct")
      stay using i +: ct
    case Event(x@GoodNews(i), ct) =>
      log.info(s"${sender.path} sends $x, $ct")
      goto(Happy)
    case Event(x@BadNews(i), ct) =>
      log.info(s"${sender.path} sends $x, $ct")
      goto(Upset)
    case Event(x@_, ct) =>
      log.info(s"${sender.path} sends $x, $ct")
      // will "stay", since no TransformHandler for NeverFeelThisWay
      goto(NeverFeelThisWay) // ==> stay
  }

  /* if no @stateTimeout, unhandled msgs would just go away.
   * Otherwise, they are hold for @stateTimeout and then queued again.
   */
  when(Happy, stateTimeout = 2.seconds) {
    case Event(x@GoodNews(i), ct) =>
      log.info(s"${sender.path} sends $x, $ct")
      stay using i +: ct
  }

  onTransition {
    //case Neutral -> Neutral => same states DON'T cause transition!
    case Neutral -> Happy =>
      log.info(s"Neutral -> Happy caused by ${sender.path}")
    case Happy -> Neutral =>
      log.info(s"Happy -> Neutral caused by ${sender.path}")
  }

  override def unhandled(message: Any): Unit = {
    log.info("unhandled msgs cause restart")
    super.unhandled(message)
  }

}
