package cf.s3.obsolete

import akka.testkit.{TestKit, TestActorRef, ImplicitSender}
import akka.actor.{ActorRef, ActorSystem}
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, WordSpecLike, Matchers}
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.{Promise, Future, Await}
import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 06/02/2014
 */
class FSMooSpec
  extends TestKit(ActorSystem("FSMooSpec"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll {
  import Person._

  implicit val ec = system.dispatcher
  override def afterAll() = system.shutdown()
  override def afterEach() = {
    println("------------------- finished one test -------------------")
  }

  def feelingShouldBe(mood: ActorRef, expected: Emo)() = {
    mood ! AskFeeling
    expectMsg(expected)
  }

  "A Neutral mood" when {
    "getting unknown msgs" should {
      "eventually stay(Neutral), with log.error though, because " +
        "when(UnsureThenStay) is not defined" in {
        val mood = TestActorRef[Mood]
        feelingShouldBe(mood, Neutral)
        mood ! "!@#$$%^..."
        feelingShouldBe(mood, Neutral)
      }
    }

    "getting Smoke-A-Joint" should {
      "goto(NeverFeelThisWay), and get stuck there" in {
        val mood = TestActorRef[Mood]
        feelingShouldBe(mood, Neutral)
        mood ! `Smoke-A-Joint`
        feelingShouldBe(mood, NeverFeelThisWay)
        mood ! GoodNews("whatever")
        feelingShouldBe(mood, NeverFeelThisWay)
        mood ! "!@#$$%^..."
        feelingShouldBe(mood, NeverFeelThisWay)
      }
    }

    // Await for @waitFor, then return a completed Future
    def sleepUntil(waitFor: FiniteDuration): Future[Unit] = {
      Try {
        Await.ready(Promise[Unit].future, waitFor)
      }
      Promise.successful({}).future
    }

    "getting GoodNews" should {
      "goto(Happy) and calm down to Neutral after stateTimeout" in {
        val mood = TestActorRef[Mood]
        feelingShouldBe(mood, Neutral)
        mood ! GoodNews("it's sunny")
        feelingShouldBe(mood, Happy)

        // choose 4 which is significantly larger than stateTimeout == 3
        /*
        // doesn't work as actor system shutdown almost immediately
        system.scheduler.scheduleOnce(4 seconds) {
          println("4 seconds latter")
          feelingShouldBe(mood, Neutral)
        }
        */
        sleepUntil(4 seconds)
        feelingShouldBe(mood, Neutral)
      }
    }

    "getting GoodNews" should {
      "goto(Happy) and get AskFeeling in 2 seconds, then calm down to " +
        "Neutral after another stateTimeout" in {
        val mood = TestActorRef[Mood]
        feelingShouldBe(mood, Neutral)
        mood ! GoodNews("it's sunny")
        feelingShouldBe(mood, Happy)
        sleepUntil(2 seconds)
        feelingShouldBe(mood, Happy) // Happy state's StateTimeout restart

        // choose 4 which is significantly larger than stateTimeout == 3
        sleepUntil(4 seconds)
        feelingShouldBe(mood, Neutral)
      }
    }

  }
}
