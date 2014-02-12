package cf.s3

import com.typesafe.config._
import akka.actor.{Props, ActorSystem}
import scala.io.Source
import akka.pattern.ask
import cf.s3.S3Put._
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Try, Failure, Success}
import scala.concurrent.{ExecutionContext, Future, Await}

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 27/01/2014
 */
object S3Client extends App {
  val appConf = ConfigFactory.load // should find application.conf
  val bucket = appConf getString "s3.bucket"
  val key = appConf getString "aws.key"
  val secret = appConf getString "aws.secret"
  val file = appConf getString "uploadTest.object"
  val objectId = appConf getString "uploadTest.objectId"

  val system = ActorSystem("S3Client")
  val s3put = system.actorOf(Props(S3Put(bucket, key, secret)), "s3Put")

  import ExecutionContext.Implicits.global
  implicit val timeout = Timeout(10 seconds)
  val f = s3put ? S3Connect
  f onComplete({
      case Success(v) => v match {
        case S3Connected =>
          println("S3Connected")
          //s3put ! S3FileUpload(file, objectId, None)
          s3put ! S3FileUpload(file, objectId, Some("binary/octet-stream"))
        case S3CommandFailed => println("S3CommandFailed")
        case x => println("S3Client: unknown msg " + x.toString)
      }
      case Failure(t) =>
        println("connect timeout")
  })

  system.scheduler.scheduleOnce(10.seconds)({
    println("Shutdown actors")
    system.stop(s3put)
    system.shutdown()
  })
}


