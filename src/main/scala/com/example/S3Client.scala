package com.example

import com.typesafe.config._
import akka.actor.{Props, ActorSystem}
import scala.io.{Codec, Source}
import akka.pattern.ask
import com.example.S3Put._
import akka.util.Timeout
import scala.concurrent.duration._
import scala.util.{Try, Failure, Success}
 /**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 27/01/2014
 */
object S3Client extends App {
  val appConf = ConfigFactory.load("application")
  val bucket = appConf getString "s3.bucket"
  val key = appConf getString "aws.key"
  val secret = appConf getString "aws.secret"
  val file = appConf getString "uploadTest.object"
  val objectId = appConf getString "uploadTest.objectId"

  val s3put = ActorSystem().actorOf(Props(S3Put(bucket, key, secret)), "s3put")

  implicit val ec = ActorSystem().dispatcher
  implicit val timeout = Timeout(10 seconds)
  val f = s3put ? S3Connect
  /*
  f andThen({
    case _ =>
  })
  */


  f.onComplete({
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

  Thread.sleep(5000)
  ActorSystem().shutdown()
}


