package com.example

import akka.actor.{Props, ActorRef, Actor, ActorLogging}
import spray.http._
import spray.http.HttpHeaders.{`Content-Length`, `Content-Type`, RawHeader, Host}
import spray.http.HttpRequest
import spray.http.ChunkedRequestStart
import spray.http.HttpHeaders.RawHeader
import scala.Some
import scala.util.{Success, Failure, Try}
import spray.can.client.ClientConnectionSettings
import akka.io.IO
import spray.can.Http
import scala.io.{Codec, Source}
import com.typesafe.config.ConfigFactory
import akka.pattern.{pipe, ask}
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.language.postfixOps

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 01/02/2014
 */
object S3StreamPut {
  def apply(bucket: String, key: String, secret: String) =
    new S3StreamPut(bucket, key,  secret)

  case class S3ChunkedStart(dest: String, contentType: Option[String],
                            contentLength: Long)
  case class S3ChunkedData(data: Array[Byte])
  case object S3ChunkedEnd
  case object S3ChunkedAck
}

class S3StreamPut(val bucket: String, val key: String, val secret: String)
  extends S3P with Actor with ActorLogging {

  import S3Put._
  import S3StreamPut._

  implicit val system = context.system        // for IO(Http)
  implicit val ec = context.system.dispatcher // for Future

  implicit val timeout = Timeout(5 seconds) // for ask pattern

  val receive = {
    val appConf = ConfigFactory.load
    if (!appConf.getBoolean("spray.can.client.chunkless-streaming"))
      doNothingAndWaitToDie("spray.can.client.chunkless-streaming " +
        "should be on. Stop self.")
    else {
      log.info("receive = waiting")
      waiting
    }
  }

  def doNothingAndWaitToDie(msg: String): Receive = {
    log.info(msg)
    // doNothing while waiting for asynchronous stop
    //context.become(doNothing)
    system.stop(self)
    // doNothing but waiting for asynchronous stop
    PartialFunction[Any, Unit]({
      case x => log.info(s"doNothing: msg $x")
    })

  }

  def waiting: Receive = {
    case S3Connect =>
      log.info("Connect")
      IO(Http) ! Http.Connect(bucketHost)

      val commander = sender
      context.become(connecting(commander))
    case x => log.info("Waiting: unknown msg" + x.toString)
  }

  def connecting(commander: ActorRef): Receive = {
    case S3Connect =>
      log.info("Connect again")
      IO(Http) ! Http.Connect(bucketHost)
      val commander = sender
      context.become(connecting(commander))
    case _ : Http.Connected =>
      log.info("Connected")
      commander ! S3Connected
      context.become(connected(commander, sender))
    case _ : Http.CommandFailed =>
      log.info("CommandFailed")
      commander ! S3CommandFailed
      context.become(waiting)
    case x => log.info("Connecting: unknown msg " + x.toString)
  }

  // S3 doesn't support "Transfer-Encoding: chunked". We need to turn on
  // "chunkless-streaming" and specify "Content-Length".
  def connected(commander: ActorRef, connection: ActorRef): Receive = {
    case S3ChunkedStart(dest, contentType, contentLength) =>
      log.info(s"S3ChunkedStart from ${sender.path}")

      val date = DateTime.now.toRfc1123DateTimeString
      val ct = properContentType(dest, contentType)
      sign(dest, date, None, Some(ct.mediaType.value),
        List("x-amz-acl:public-read")) map({ sig =>
        val auth = s"AWS $key:$sig"
        log.info("""auth="%s"""" format auth)
        ChunkedRequestStart(HttpRequest(HttpMethods.PUT, "/" + dest,
          List(Host(bucketHost),
            RawHeader("Date", date),
            `Content-Type`(ct),
            RawHeader("x-amz-acl", "public-read"),
            RawHeader("Authorization", auth),
            `Content-Length`(contentLength)
          )))
        }) map { req =>
          /* Ask pattern doesn't work here because:
           * 1. A temporary actor is generated and stands in the middle of
           * asker and askee.
           * 2. spray-can remembers the sender when deal with
           * ChunkedRequestStart and subsequently ack and response to that
           * remembered sender.
           * 3. Therefore, spray-can acts as a askee and remembers the temp
           * actor and acks and responses everything to it(by then it's become
           * deadLetter).
           */
          connection ! req.withAck(S3ChunkedAck)
        }
    case S3ChunkedData(data: Array[Byte]) =>
      log.info(s"S3ChunkedData from ${sender.path} to ${connection.path}")
      connection ! MessageChunk(data).withAck(S3ChunkedAck)

    case S3ChunkedAck =>
      log.info(s"S3ChunkedAck from ${sender.path} to ${commander.path}")
      commander ! S3ChunkedAck

    case S3ChunkedEnd =>
      log.info(s"S3ChunkedEnd from ${sender.path}")
      connection ! ChunkedMessageEnd.withAck(S3ChunkedAck)

    case x => log.info("Connected: unknown msg " + x.toString)
  }

}
