package com.example

import scala.util.{Failure, Success, Try}
import com.typesafe.config.ConfigFactory
import akka.actor._
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps
import com.example.S3Put._
import akka.pattern.{ask, pipe}
import scala.concurrent.ExecutionContext
import com.example.S3StreamPut._
import scala.io.{Codec, Source}
import spray.http.HttpResponse
import spray.http.HttpResponse
import com.example.S3StreamPut.S3ChunkedData
import com.example.S3StreamPut.S3ChunkedStart

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 01/02/2014
 */
object S3StreamClient extends App {
  val appConf = ConfigFactory.load
  val bucket = appConf getString "s3.bucket"
  val key = appConf getString "aws.key"
  val secret = appConf getString "aws.secret"

  val system = ActorSystem("S3StreamClient")
  val s3client = system.actorOf(Props(
    new S3StreamClient(bucket, key, secret)), "s3StreamClient")

  import ExecutionContext.Implicits.global
  system.scheduler.scheduleOnce(10.seconds)({
    println("Shutdown actors")
    //system.stop(s3client)
    system.shutdown()
  })
}

class S3StreamClient(val bucket: String, val key: String, val secret: String)
  extends Actor with ActorLogging {

  val appConf = ConfigFactory.load("application")
  val file = appConf getString "uploadTest.object"
  val objectId = appConf getString "uploadTest.objectId"

  /*
  val s3put = context.system.actorOf(Props(S3StreamPut(bucket, key, secret)),
    "s3StreamPut")
    */
  val s3put = context.system.actorOf(Props(S3StreamPutFSM(bucket, key, secret)),
    "s3StreamPutFSM")

  val chunkSize = 1024 * 10
  val buf = {
    val src = Source.fromFile(file)(Codec.ISO8859)
    val buf = src.map(_.toByte).toArray
    src.close
    buf
  }

  implicit val ec = ActorSystem().dispatcher
  implicit val timeout = Timeout(10 seconds)

  s3put ! S3Connect

  def receive: Receive = {
    case S3Connected =>
      log.info("S3Connected")
      //s3put ! S3FileUpload(file, objectId, None)
      s3put ! S3ChunkedStart(objectId, None, buf.length)
      context.become(transferring(buf))
    case S3CommandFailed =>
      doNothingAndWaitToDie("S3CommandFailed")
    case x => log.info("receive: unknown msg " + x.toString)
  }

  def transferring(data: Array[Byte]): Receive = {
    case S3ChunkedAck =>
      log.info("S3ChunkedAck back")
      if (data.length > chunkSize) {
        s3put ! S3ChunkedData(data.take(chunkSize))
        context.become(transferring(data.drop(chunkSize)))
      } else {
        s3put ! S3ChunkedData(data)
        context.become(transferred)
      }
    case x => log.info("transferring: unknown msg " + x.toString)
  }

  def transferred: Receive = {
    case S3ChunkedAck =>
      log.info("S3ChunkedAck back from S3ChunkedData")
      s3put ! S3ChunkedEnd
      // TODO: killself
    case x: HttpResponse =>
      log.info(s"transferred: $x")
    case x => log.info("transferred: unknown msg " + x.toString)
  }


  def doNothingAndWaitToDie(msg: String) {
    log.info(msg)
    // doNothing while waiting for asynchronous stop
    context.become(doNothing)
    context.system.stop(self)
  }

  // doNothing while waiting for asynchronous stop
  def doNothing: Receive = { case x => log.info("doNothing: msg" + x.toString) }

}
