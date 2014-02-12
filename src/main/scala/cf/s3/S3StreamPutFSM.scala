package cf.s3

import cf.s3.S3Put._
import cf.s3.S3StreamPut._
import akka.actor._
import com.typesafe.config.ConfigFactory
import akka.io.IO
import spray.can.Http
import spray.can.Http.{ConnectionClosed, Connected, CommandFailed}
import spray.http._
import spray.http.HttpHeaders.{`Content-Length`, `Content-Type`, RawHeader, Host}
import spray.http.HttpRequest
import spray.http.ChunkedRequestStart
import spray.http.HttpHeaders.RawHeader
import scala.Some
import cf.s3.S3StreamPutFSM.FSMData

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 06/02/2014
 */
object S3StreamPutFSM {
  def apply(bucket: String, key: String, secret: String) =
    new S3StreamPutFSM(bucket, key,  secret)

  trait S3State
  case object S3Waiting extends S3State
  case object S3Connecting extends S3State
  case object S3WaitingStart extends S3State
  case object S3WaitingStartAck extends S3State
  case object S3WaitingChunk extends S3State
  case object S3WaitingChunkAck extends S3State
  case object S3WaitingRes extends S3State

  case class FSMData(connection: Option[ActorRef] = None,
                     commander: Option[ActorRef] = None)

  case object S3ForceRestart extends S3Command

}

class S3StreamPutFSM(val bucket: String, val key: String, val secret: String)
  extends S3P with Actor with Stash
  with FSM[S3StreamPutFSM.S3State, Option[FSMData]]
  with LoggingFSM[S3StreamPutFSM.S3State, Option[FSMData]]
  with ActorLogging {

  import S3StreamPutFSM._
  implicit val system = context.system        // for IO(Http)
  implicit val ec = context.system.dispatcher // for Future

  val goodToGo: Boolean = {
    val appConf = ConfigFactory.load
    appConf.getBoolean("spray.can.client.chunkless-streaming")
  }

  startWith(S3Waiting, None)

  when(S3Waiting) {
    case Event(S3Connect, _) =>
      if (goodToGo) {
        IO(Http) ! Http.Connect(bucketHost)
        goto(S3Connecting) using Some(FSMData(commander = Some(sender)))
      }
      else stay
  }

  /* Only explicitly handle Tcp.CommandFailed in this case.
   * In other states, an unhandled CommandFailed causes a restart.
   */
  when(S3Connecting) {
    case Event(_: CommandFailed, Some(FSMData(_, Some(commander)))) =>
      commander ! S3CommandFailed
      goto(S3Waiting) using None
    case Event(_: Connected, Some(FSMData(_, Some(commander)))) =>
      commander ! S3Connected
      context.watch(sender)
      goto(S3WaitingStart) using Some(FSMData(connection = Some(sender)))
    case Event(_: S3ChunkedStart | S3ChunkedData(_) | S3ChunkedEnd, _) =>
      sender ! S3CommandFailed
      stay
    /* Duplicated S3Connect would be silently ignored. Since some clients are
     * so impatient.
     */
  }

  when(S3WaitingStart) {
    case Event(S3Connect, _) =>
        IO(Http) ! Http.Connect(bucketHost)
        goto(S3Connecting) using Some(FSMData(commander = Some(sender)))
    case Event(x: S3ChunkedStart, Some(fsmdata)) =>
      sendS3ChunkedStart(x, fsmdata.connection.get)
      // update commander, all ack is forwarded to this commander
      goto (S3WaitingStartAck) using
        Some(fsmdata.copy(commander = Some(sender)))
    case Event(_: S3ChunkedData | S3ChunkedEnd, _) =>
      sender ! S3CommandFailed
      stay
  }

  when(S3WaitingStartAck) {
    case Event(S3Connect | S3ChunkedStart | S3ChunkedData | S3ChunkedEnd, _) =>
      sender ! S3CommandFailed
      stay
    case Event(S3ChunkedAck, Some(fsmdata)) =>
      log.info(s"S3ChunkedAck: $fsmdata")
      fsmdata.commander.get ! S3ChunkedAck
      goto(S3WaitingChunk)
  }

  when(S3WaitingChunk) {
    case Event(S3Connect | S3ChunkedStart, _) =>
      sender ! S3CommandFailed
      stay
    case Event(S3ChunkedData(data), Some(fsmdata)) =>
      fsmdata.connection.get ! MessageChunk(data).withAck(S3ChunkedAck)
      goto(S3WaitingChunkAck) using
        Some(fsmdata.copy(commander = Some(sender)))
    case Event(S3ChunkedEnd, Some(fsmdata)) =>
      /* It seems that spray directly acks ChunkedMessageEnd with Http response
       * instead of withAck
       */
      fsmdata.connection.get ! ChunkedMessageEnd.withAck(S3ChunkedAck)
      goto(S3WaitingRes) using
        Some(fsmdata.copy(commander = Some(sender)))
  }

  when(S3WaitingChunkAck) {
    case Event(S3Connect | S3ChunkedStart | S3ChunkedData, _) =>
      sender ! S3CommandFailed
      stay
    case Event(S3ChunkedEnd, Some(fsmdata)) =>
      /* allow client to send S3ChunkedEnd w/o waiting for the ack of the
       * last S3ChunkedData
       */
      stash()
      stay
    case Event(S3ChunkedAck, Some(fsmdata)) =>
      log.info(s"S3ChunkedAck from ${sender.path} to ${fsmdata.commander.get.path}")
      fsmdata.commander.get ! S3ChunkedAck
      goto(S3WaitingChunk)
  }

  when(S3WaitingRes) {
    case Event(S3Connect | S3ChunkedStart | S3ChunkedData | S3ChunkedEnd, _) =>
      sender ! S3CommandFailed
      stay
    case Event(x: HttpResponse, Some(fsmdata)) =>
      fsmdata.commander.get ! x
      goto(S3WaitingStart) using Some(FSMData(connection = Some(sender)))
  }

  whenUnhandled {
    case Event(x@(S3ForceRestart | CommandFailed | _: ConnectionClosed), ct) =>
      /* CommandFailed might happen during writing. ConnectionClosed might
       * happen anytime. Had them happen and not been handled would throw and
       * therefore a restart.
       */
      log.info(s"[$stateName]: ${sender.path} sends $x, $ct")
      throw new Exception()
    case Event(x@Terminated(connection), ct) =>
      log.info(s"[$stateName]: ${connection.path} terminated, $ct")
      if (!ct.isEmpty && !ct.get.commander.isEmpty) {
        // if termination happens during any ack waiting states
        ct.get.commander.get ! S3CommandFailed
      }
      goto(S3Waiting) using None
    case Event(x, ct) =>
      log.info(s"[$stateName]: ${sender.path} sends $x, $ct")
      stay
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
}
