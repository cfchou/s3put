package com.example

import akka.actor._
import spray.http._
import spray.http.HttpHeaders.{`Content-Length`, `Content-Type`, RawHeader, Host}
import scala.util.{Success, Failure, Try}
import akka.io.IO
import spray.can.Http
import com.typesafe.config.ConfigFactory
import akka.pattern.{pipe, ask}
import scala.concurrent.Await
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps
import spray.can.Http.{Connected, CommandFailed}
import com.example.S3Put._
import spray.http.{HttpRequest, ChunkedRequestStart, HttpResponse}
import scala.Some

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 01/02/2014
 */
object S3StreamPut {
  def apply(bucket: String, key: String, secret: String) =
    new S3StreamPut(bucket, key,  secret)

  trait S3ChunkCommand extends S3Command
  case class S3ChunkedStart(dest: String,
                            contentType: Option[String],
                            contentLength: Long) extends S3ChunkCommand
  case class S3ChunkedData(data: Array[Byte]) extends S3ChunkCommand
  case object S3ChunkedEnd extends S3ChunkCommand
  case object S3ChunkedAck extends S3CommandResult
  case class S3CommandFailedId(id: String) extends S3CommandResult
}

class S3StreamPut(val bucket: String, val key: String, val secret: String)
  extends S3P with Actor with Stash with ActorLogging {

  import S3StreamPut._

  implicit val system = context.system        // for IO(Http)
  implicit val ec = context.system.dispatcher // for Future

  implicit val timeout = Timeout(5 seconds) // for ask pattern

  val receive = {
    val appConf = ConfigFactory.load
    if (!appConf.getBoolean("spray.can.client.chunkless-streaming"))
      toFailEverything("spray.can.client.chunkless-streaming " +
        "should be on. Stop self.")
    else {
      log.info("receive = waiting")
      waiting
    }
  }

  def toFailEverything(msg: String): Receive = {
    log.info(msg)
    // doNothing while waiting for asynchronous stop
    //context.become(doNothing)
    system.stop(self)
    // doNothing but waiting for asynchronous stop
    PartialFunction[Any, Unit]({
      case x =>
        log.info(s"toFailEverything: $x")
        sender ! CommandFailed
    })

  }

  def waiting: Receive = {
    case S3Connect =>
      log.info("Connect")
      IO(Http) ! Http.Connect(bucketHost)

      val commander = sender
      context.become(connecting(commander))
    case x => log.warning("Waiting: unexpected msg" + x.toString)
  }

  def connecting(commander: ActorRef): Receive = {
    case S3Connect =>
      log.info("Connect again")
      IO(Http) ! Http.Connect(bucketHost)
      val commander = sender
      context.become(connecting(commander))
    case _: Connected =>
      log.info("Connected")
      commander ! S3Connected
      context.become(connected(commander, sender, false))
    case x: CommandFailed =>
      log.error("connecting: " + x.toString)
      commander ! S3CommandFailed
      // better to restart, so that won't get msgs from previous connection.
      // as per the default SupervisorStrategy, Exception will causes restart.
      throw new Exception()
      //context.become(waiting)
    case x => log.warning("connecting: unknown msg " + x.toString)
  }

  def sendS3ChunkedStart(msg: S3ChunkedStart,
                         connection: ActorRef): Unit = {
    val (dest, contentType, contentLength) =
      (msg.dest, msg.contentType, msg.contentLength)
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
  }

  // S3 doesn't support "Transfer-Encoding: chunked". We need to turn on
  // "chunkless-streaming" and specify "Content-Length".
  def connected(commander: ActorRef, connection: ActorRef, waitAck: Boolean)
  : Receive = {
    case x: S3ChunkedStart =>
      if (waitAck) {
        // client sends multiple msgs with same id will get only one result
        log.warning(s"Unexpected $x")
      } else {
        log.info(s"S3ChunkedStart from ${sender.path}")
        sendS3ChunkedStart(x, connection)
        context.become(connected(commander, connection, true))
      }
    case S3ChunkedAck =>
      log.info(s"S3ChunkedAck from ${sender.path} to ${commander.path}")
      commander ! S3ChunkedAck
      log.info(s"waiting_data(${commander.path}, ${connection.path})")
      context.become(waiting_data(commander, connection, false))
    case x: S3ChunkCommand =>
        // client sends multiple msgs with same id will get only one result
        log.info(s"$x before/during S3ChunkedStart, ignore")
    case x: CommandFailed =>
      log.error(s"connecting(waitAck==$waitAck): $x")
      commander ! S3CommandFailed
      throw new Exception()
    case x => log.warning("connected: unknown msg" + x.toString)
  }


  // handle S3ChunkedData* until S3ChunkedEnd
  def waiting_data(commander: ActorRef, connection: ActorRef, waitAck: Boolean)
  : Receive = {
    case x: S3ChunkedStart =>
      log.warning(s"Unexpected $x")

    case S3ChunkedData(data: Array[Byte]) =>
      if (waitAck) {
        log.warning(s"S3ChuckedData before ack, ignore")
      } else {
        log.info(s"S3ChunkedData from ${sender.path} to ${connection.path}")
        connection ! MessageChunk(data).withAck(S3ChunkedAck)
        context.become(waiting_data(commander, connection, true))
      }
    case S3ChunkedEnd =>
      if (waitAck) {
        // allow client to send S3ChunkedEnd w/o waiting for the ack of the
        // last S3ChunkedData
        log.warning(s"S3ChuckedEnd before ack, stash")
        stash()
      } else {
        log.info(s"S3ChunkedEnd from ${sender.path}")
        connection ! ChunkedMessageEnd.withAck(S3ChunkedAck)
        log.info(s"waiting_endack(${commander.path}, ${connection.path})")
        // It seems that spray doesn't ack for ChunkedMessageEnd
        //context.become(waiting_endack(commander, connection))
        context.become(waiting_response(commander, connection))
      }
    case S3ChunkedAck =>
      if (waitAck) {
        log.info(s"S3ChunkedAck from ${sender.path} to ${commander.path}")
        commander ! S3ChunkedAck
        unstashAll()
        context.become(waiting_data(commander, connection, false))
      } else {
        // one MessageChunk is pair with one ack only and no overlapping should
        // happen between pairs
        log.warning("Unexpected S3ChunkedAck, drop it")
      }
    case x: CommandFailed =>
      log.error(s"waiting_data(waitAck==$waitAck): $x")
      commander ! S3CommandFailed
      throw new Exception()
    case x => log.warning("waiting_data: unknown msg " + x.toString)
  }

  // It seems that spray doesn't ack for ChunkedMessageEnd
  def waiting_endack(commander: ActorRef, connection: ActorRef): Receive = {
    case S3ChunkedAck =>
      log.info(s"S3ChunkedAck from ${sender.path} to ${commander.path}")
      commander ! S3ChunkedAck
      context.become(waiting_response(commander, sender))
    case x: S3ChunkCommand =>
      log.warning(s"Unexpected $x")
    case x: CommandFailed =>
      log.error(s"waiting_endack: $x")
      commander ! S3CommandFailed
      throw new Exception()
    case x => log.warning("waiting_endack: unknown msg " + x.toString)
  }

  def waiting_response(commander: ActorRef, connection: ActorRef): Receive = {
    case x: S3ChunkCommand =>
      log.warning(s"Unexpected $x")
    case x: HttpResponse =>
      log.info(s"$x")
      commander ! x
      context.become(connected(commander, connection, false))
    case x: CommandFailed =>
      log.error(s"waiting_response: $x")
      commander ! S3CommandFailed
      throw new Exception()
    case x => log.warning("waiting_response: Unknown msg" + x.toString)
  }

}
