package com.example

import akka.actor.{ActorRef, ActorLogging, Actor}
import akka.io.IO
import spray.can.Http
import akka.pattern.{ask, pipe}
import scala.concurrent.{Await, ExecutionContext, Future}
import spray.http._
import spray.httpx.RequestBuilding
import spray.httpx.RequestBuilding._
import scala.io.{Codec, Source}
import spray.httpx.marshalling.Marshaller
import spray.httpx.marshalling.BasicMarshallers.byteArrayMarshaller
import spray.http.HttpRequest

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 28/01/2014
 */
object S3Put {
  def apply(bucket: String, key: String, secret: String) =
    new S3Put(bucket, key,  secret)

  sealed trait S3Command
  case object S3Connect extends S3Command
  // @dest is the path of the object on S3, without the starting '/'. E.g.
  // "/bucketname/dir/to/object", @dest = "dir/to/object"
  case class S3FileUpload(file: String, dest: String, contentType: Option[String])


  sealed trait S3CommandResult
  case object S3Connected extends S3CommandResult
  case object S3FileUploadAck extends S3CommandResult
  case object S3CommandFailed extends S3CommandResult
}

class S3Put(val bucket: String, val key: String, val secret: String)
  extends S3P with Actor with ActorLogging {

  import com.example.S3Put._

  implicit val system = context.system        // for IO(Http)
  implicit val ec = context.system.dispatcher // for Future

  def createRequest(bytes: Array[Byte], dest: String, contentType: Option[String])
  : Future[HttpRequest] = {
    val date = DateTime.now.toRfc1123DateTimeString
    val ct = properContentType(dest, contentType)
    sign(dest, date, None, Some(ct.mediaType.value),
      List("x-amz-acl:public-read")) map({ sig =>
      val auth = "AWS %s:%s" format (key, sig)
      log.info("""auth="%s"""" format auth)

      //val marshaller = implicitly[Marshaller[Array[Byte]]]
      implicit val marshaller = byteArrayMarshaller(ct)
      RequestBuilding.Put("/" + dest, bytes) ~>
        addHeader("Host", bucketHost) ~>
        addHeader("Date", date) ~>
        // "Content-Type" is automatically added by ByteArrayMarshaller
        //addHeader("Content-Type", contentType.getOrElse("application/octet-stream")) ~>
        addHeader("x-amz-acl", "public-read") ~>
        addHeader("Authorization", auth)
      // "Content-Length" is added automatically by spray-can
    })
  }

  def receive: Receive = waiting

  def waiting: Receive = {
    case S3Connect =>
      log.info("Connect")
      //IO(Http) ! Http.Connect(bucketHost, sslEncryption = true)
      IO(Http) ! Http.Connect(bucketHost)
      val commander = sender
      context.become(connecting(commander))
    /*
    case S3Connect =>
      log.info("Connect")
      val commander = sender
      implicit val timeout = Timeout(10 seconds)
      implicit val ec = context.system.dispatcher
      val f = IO(Http) ? Http.Connect("%s.s3.amazonaws.com" format bucket, sslEncryption = true)
      f pipeTo(commander)
    */
    case x => log.info("Waiting: unknown msg" + x.toString)
  }


  def connecting(commander: ActorRef): Receive = {
    case S3Connect =>
      log.info("Connecting: Connect again")
      //IO(Http) ! Http.Connect(bucketHost, sslEncryption = true)
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

  def connected(commander: ActorRef, connection: ActorRef): Receive = {
    case S3FileUpload(file, dest, ct) =>
      log.info("S3FileUpload")
      val src = Source.fromFile(file)(Codec.ISO8859)
      val buf = src.map(_.toByte).toArray
      src.close
      createRequest(buf, dest, ct) map ({ connection ! _})
    case x => log.info("Connected: unknown msg " + x.toString)
  }
}



