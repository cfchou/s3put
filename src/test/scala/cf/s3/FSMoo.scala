package cf.s3

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
  case object UnsureThenStay extends Emo

  type Content = List[String]

  trait News
  case object Nothing extends News
  case class Info(data: String) extends News
  case class GoodNews(data: String) extends News
  case class BadNews(data: String) extends News
  case object `Smoke-A-Joint` extends News
  // query one's Emo
  case object AskFeeling extends News
}

class Person(mood: ActorRef) extends Actor with ActorLogging {
  import Person._

  val system = context.system
  implicit val ec = system.dispatcher
  implicit val timeout = Timeout(1 second)

  mood ! Nothing
  mood ! Info("today is wednesday")
  mood ! GoodNews("sunny day")
  //mood ! Nothing
  //mood ! "@#$%^..."
  /*
  system.scheduler.scheduleOnce(5 second) {
    mood ! GoodNews("good food")
  }
  */
  system.scheduler.schedule(1 second, 1 second) {
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

  /*
   * If goto(UnsureThenStay), log.error appears because no when(UnsureThenStay)
   * is declared. Eventually stay().
   *
   * If goto(NeverFeelThisWay), no log.error because when(NeverFeelThisWay) is
   * defined. But actor will be stuck in NeverFeelThisWay unless whenUnhandled
   * could help.
   */
  when(NeverFeelThisWay)(FSM.NullFunction)


  when(Neutral) (ansCurrentState orElse {
    case Event(x@AskFeeling, ct) =>
      // this is designed for answering current state
      log.info(s"${sender.path} sends $x, $ct")
      sender ! stateName
      stay
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
    case Event(x@`Smoke-A-Joint`, ct) =>
      goto(NeverFeelThisWay)
    case Event(x@_, ct) => // capture all unknown msgs
      log.info(s"${sender.path} sends $x, $ct")
      // Will "stay"(with log.error) if no TransformHander and FSM.NullFunction
      // defined for NeverFeelThisWay.
      goto(UnsureThenStay) // ==> stay?
  })

  /* using val before defining them results in NullPointerException.
   * so define ansCurrentState before using them, or use def instead.
   */
  //val ansCurrentState: this.StateFunction = ???
  def ansCurrentState: this.StateFunction = {
    case Event(x@AskFeeling, ct) =>
      // this is designed for answering current state
      log.info(s"${sender.path} sends $x, $ct")
      sender ! stateName
      stay
  }


  // whenUnhandled is unstackable, latter one overrides the former
  whenUnhandled (ansCurrentState orElse {
    case Event(x@AskFeeling, ct) =>
      // this is designed for answering current state
      log.info(s"${sender.path} sends $x, $ct")
      sender ! stateName
      stay
    case Event(x, ct) if (stateName == Neutral) =>
      log.error(s"Neutral sees unhandled Event $x, impossible as per our code!")
      throw new Exception()
    case Event(x, ct) if (stateName == Happy) =>
      log.warning(s"Happy sees unhandled Event $x")
      stay
  })

  /* A StateTimeout message comes in every @stateTimeout.
   * Restart the alarm everytime any Event comes in.
   */
  when(Happy, stateTimeout = 3.seconds) {
    case Event(x@StateTimeout, ct) =>
      log.info(s"${sender.path} sends $x, $ct")
      goto(Neutral) using List.empty
    case Event(x@GoodNews(i), ct) =>
      log.info(s"${sender.path} sends $x, $ct")
      stay using i +: ct
  }

  onTransition {
    case x -> y if (x == y) =>
      // should never be here! stay or goto(sameState) will not cause
      // transition
      log.error(s"same states($stateName) should NOT cause transition!")
    case _ -> NeverFeelThisWay =>
      log.info(s"$stateName -> NeverFeelThisWay caused by ${sender.path}" +
        "will be stuck there!!")
    case Neutral -> Happy =>
      log.info(s"Neutral -> Happy caused by ${sender.path}")
    case Happy -> Neutral =>
      log.info(s"Happy -> Neutral caused by ${sender.path}")
  }


}
