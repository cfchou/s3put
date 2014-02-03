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
  case class S3ChunkedAckTo(commander: ActorRef)
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


/*
  // S3 doesn't support "Transfer-Encoding: chunked". We need to turn on
  // "chunkless-streaming" and specify "Content-Length".
  def chunkActor(connection: ActorRef): ActorRef = {
    system.actorOf(Props(new Actor with ActorLogging {
      def receive: Receive = {
        case S3ChunkedStart(dest, contentType, contentLength) =>
          log.info(s"S3ChunkedStart from ${sender.path}")

          val date = DateTime.now.toRfc1123DateTimeString
          val ct = properContentType(dest, contentType)
          val req = sign(dest, date, None, Some(ct.mediaType.value),
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
          }) flatMap { req =>
            connection ? req.withAck(S3ChunkedAck(sender)) pipeTo(sender)
          }
          Await.ready(req, 1 second)
        case S3ChunkedData(data: Array[Byte]) =>
          //connection ! MessageChunk(data).withAck(S3ChunkedAck(sender))
          Await.ready({
            (connection ? MessageChunk(data).withAck(S3ChunkedAck(sender))).
              pipeTo(sender)
          }, 1 second)
        case S3ChunkedEnd =>
          //connection ! ChunkedMessageEnd.withAck(S3ChunkedAck(sender))
          Await.ready({
            (connection ? ChunkedMessageEnd.withAck(S3ChunkedAck(sender))).
              pipeTo(sender)
          }, 1 second)
        /*
        case S3ChunkedAck(commander2) =>
          // commander is the S3StreamPut actor
          log.info(s"S3ChunkedAcked to ${commander.path}")
          commander ! S3ChunkedAck(system.deadLetters)
        */
        case x => log.info("chunkActor: unknown msg" + x.toString)
      }
    }), "chunker")
  }
*/


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
      val start = sign(dest, date, None, Some(ct.mediaType.value),
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
        connection ! req.withAck(S3ChunkedAck)
      }
      /*
      }) flatMap { req =>
        connection ? req.withAck(S3ChunkedAck) pipeTo(sender)
      }
      */
      Await.ready(start, 1 second)
    case S3ChunkedData(data: Array[Byte]) =>
      log.info(s"S3ChunkedData from ${sender.path} to ${connection.path}")
      //connection ! MessageChunk(data)
      //connection ! MessageChunk(data).withAck(S3ChunkedAckTo(sender))
      connection ! MessageChunk(data).withAck(S3ChunkedAck)

      /*
      log.info(s"implicit timeout ${implicitly[Timeout]}")

      Await.ready({
        connection ? MessageChunk(data).withAck(S3ChunkedAck) transform({
          case x =>
            log.info(s"MessageChunk Ack $x")
            x
        }, {
          case e =>
            log.info(s"MessageChunk Ex $e")
            e
        }) pipeTo(sender)
      }, 5 seconds)
      */
    case S3ChunkedAck =>
      log.info(s"S3ChunkedAck from ${sender.path} to ${commander.path}")
      commander ! S3ChunkedAck

    case S3ChunkedAckTo(commander) =>
      log.info(s"S3ChunkedAckTo from ${sender.path} to ${commander.path}")
      commander ! S3ChunkedAck
    case S3ChunkedEnd =>
      log.info(s"S3ChunkedEnd from ${sender.path}")
      //connection ! ChunkedMessageEnd.withAck(S3ChunkedAckTo(sender))
      connection ! ChunkedMessageEnd.withAck(S3ChunkedAck)

      /*
      Await.ready({
        connection ? ChunkedMessageEnd.withAck(S3ChunkedAck) pipeTo(sender)
      }, 1 second)
      */

    case x => log.info("Connected: unknown msg " + x.toString)
  }

  /*
  def connected(connection: ActorRef): Receive = {
    case S3ChunkedStart(dest, ct, cl) =>
      log.info("S3ChunkedStart")
      val chunker = chunkActor(connection)
      // Bypassing to client the S3ChunkedAck that coming back
      // (chunker ? S3ChunkedStart(dest, ct, cl)) pipeTo(sender)

      (chunker ? S3ChunkedStart(dest, ct, cl)) transform({ x =>
        log.info(s"going to pipe this: $x")
        x
      }, { e =>
        log.info(s"got exception: $e")
        e
      }) pipeTo(sender)
      context.become(transferring(chunker))
    case x => log.info("Connected: unknown msg " + x.toString)
  }

  def transferring(chunker: ActorRef): Receive = {
    case S3ChunkedData(data: Array[Byte]) =>
      log.info("S3ChunkedData")
      (chunker ? S3ChunkedData(data)) pipeTo(sender)
    case S3ChunkedEnd =>
      log.info("S3ChunkedEnd")
      (chunker ? S3ChunkedEnd) pipeTo(sender) onComplete ({ _ =>
        system.stop(chunker) // chunker can be GC-ed
      })
      context.become(waiting)
    case x => log.info("Transferring: unknown msg " + x.toString)
  }
  */
}
