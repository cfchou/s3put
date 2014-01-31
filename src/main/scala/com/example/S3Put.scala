package com.example

import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import akka.util.Timeout
import scala.concurrent.duration._
import akka.io.IO
import spray.can.Http
import akka.pattern.{ask, pipe}
import scala.concurrent.{Await, ExecutionContext, Future}
import spray.http._
import spray.http.HttpHeaders._
import spray.httpx.RequestBuilding
import spray.httpx.RequestBuilding._
import scala.io.{Codec, Source}
import spray.httpx.marshalling.Marshaller
import spray.httpx.marshalling.BasicMarshallers.byteArrayMarshaller
import spray.http.HttpRequest
import com.typesafe.config.ConfigFactory
import spray.can.client.ClientConnectionSettings
import scala.util.{Success, Failure, Try}
import spray.http.HttpHeaders.RawHeader
import scala.util.Failure
import scala.Some
import scala.util.Success
import spray.http.HttpRequest
import spray.http.ChunkedRequestStart

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

  case class S3ChunkedStart(dest: String, contentType: Option[String],
                            contentLength: Long)
  case class S3ChunkedData(data: Array[Byte])
  case object S3ChunkedEnd
  case class S3ChunkedAck(commander: ActorRef)

  sealed trait S3CommandResult
  case object S3Connected extends S3CommandResult
  case object S3CommandFailed extends S3CommandResult
}

trait S3P {
  val bucket: String
  val key: String
  val secret: String
  require(!bucket.isEmpty && !key.isEmpty && !secret.isEmpty)
}

class S3Put(bucket: String, key: String, secret: String)
  extends Actor with ActorLogging {

  import com.example.S3Put._
  implicit val system = context.system        // for IO(Http)
  implicit val ec = context.system.dispatcher // for Future

  val appConf = ConfigFactory.load("application")

  val bucketHost = "%s.s3.amazonaws.com" format bucket
  //val bucketHost = "httpbin.org"

  // TODO
  def canonicalizedAmzHeaders(amzHeaders: List[String]): List[String] = {
    amzHeaders.map(_.toLowerCase).find(_.startsWith("x-amz-acl:")) match {
      case None => List.empty
      case Some(h) => List(h)
    }
  }

  // TODO
  // virtual hosted style, no subresource
  def canonicalizedResource(dest: String): String = "/" + bucket + "/" + dest

  def sign(dest: String, date: String,
           contentMD5: Option[String], contentType: Option[String],
           amzHeaders: List[String]): Future[String] = Future {
    val message = (List("PUT", contentMD5.getOrElse(""),
      contentType.getOrElse("application/octet-stream"), date) ++
      canonicalizedAmzHeaders(amzHeaders) :+
      canonicalizedResource(dest)).mkString("\n")
    log.info(message)

    val mac = Mac.getInstance("HmacSHA1")
    mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA1"))
    new String(Base64.encodeBase64(mac.doFinal(message.getBytes("UTF-8"))))
  }

  def dynamicMarshaller(cT: ContentType, more: ContentType*) = {
    Marshaller.of[Array[Byte]](cT +: more: _*)({(arr, ct, ctx) =>
    })
  }

  // charset in @contentType is ignored. e.g.
  // Some("text/html; charset=ISO-8859-4") would be treated as Some("text/html")
  def properContentType(dest: String, contentType: Option[String]): ContentType =
    contentType match {
      case None =>
        // TODO:
        // val ext = get file extension from dest
        val ext = ""
        MediaTypes.forExtension(ext).
          fold(defaultContentType)(ContentType.apply _)
      case Some(ct) =>
        val part = ct.takeWhile(';' != _).split("/")
        if (part.length != 2) defaultContentType
        else contentTypeByMediaType(part(0), part(1))
    }

  def contentTypeByMediaType(mainType: String, subType: String): ContentType = {
    MediaTypes.getForKey(mainType, subType).fold({
      val x = MediaType.custom(mainType, subType)
      MediaTypes.register(x)
      ContentType(x)
    })(ContentType.apply _)
  }

  val defaultContentType: ContentType =  ContentTypes.`application/octet-stream`

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

  // S3 doesn't support "Transfer-Encoding: chunked". We need to turn on
  // "chunkless-streaming" and specify "Content-Length".
  def chunkActor(connection: ActorRef): ActorRef = {
    system.actorOf(Props(new Actor with ActorLogging {
      def receive: Receive = {
        case S3ChunkedStart(dest, contentType, contentLength) =>
          val date = DateTime.now.toRfc1123DateTimeString
          val ct = properContentType(dest, contentType)
          val req = sign(dest, date, None, Some(ct.mediaType.value),
            List("x-amz-acl:public-read")) map({ sig =>
            val auth = "AWS %s:%s" format (key, sig)
            log.info("""auth="%s"""" format auth)
            ChunkedRequestStart(HttpRequest(HttpMethods.PUT, "/" + dest,
              List(Host(bucketHost),
                RawHeader("Date", date),
                `Content-Type`(ct),
                RawHeader("x-amz-acl", "public-read"),
                RawHeader("Authorization", auth),
                `Content-Length`(contentLength)
              )))
          }) map (connection ! _.withAck(S3ChunkedAck(sender)))
          //Await.ready(req, 1.second)
        case S3ChunkedData(data: Array[Byte]) =>
            connection ! MessageChunk(data).withAck(S3ChunkedAck(sender))
        case S3ChunkedEnd =>
          connection ! ChunkedMessageEnd.withAck(S3ChunkedAck(sender))
        case S3ChunkedAck(commander) =>
          log.info("S3ChunkedAcked")
          commander ! S3ChunkedAck(system.deadLetters)
        case x => log.info("chunkActor: unknown msg" + x.toString)
      }
    }), "chunkedRequest")
  }

  def receive: Receive = waiting

  def waiting: Receive = {
    case S3Connect =>
      log.info("Connect")
      //IO(Http) ! Http.Connect(bucketHost, sslEncryption = true)
      //IO(Http) ! Http.Connect(bucketHost)

      val appConf = Try({ConfigFactory.load("application")})
      val chunked = appConf.flatMap(c =>
        Try({c.getBoolean("chuncked-message")}).flatMap({ b =>
          if (b) Try({ Some(ClientConnectionSettings(
            c.getConfig("spray.can.client.chunkless-streaming"))) })
          else Try(None)
      })) match {
        case Failure(_) => None
        case Success(v) => v
      }

      IO(Http) ! Http.Connect(bucketHost, settings = chunked)
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
      log.info("Connect again")
      //IO(Http) ! Http.Connect(bucketHost, sslEncryption = true)
      IO(Http) ! Http.Connect(bucketHost)
      val commander = sender
      context.become(connecting(commander))
    case _ : Http.Connected =>
      log.info("Connected")
      commander ! S3Connected
      context.become(connected(sender))
    case _ : Http.CommandFailed =>
      log.info("CommandFailed")
      commander ! S3CommandFailed
      context.become(waiting)
    case x => log.info("Connecting: unknown msg " + x.toString)
  }

  def connected(connection: ActorRef): Receive = {
    case S3FileUpload(file, dest, ct) =>
      log.info("S3FileUpload")
      val src = Source.fromFile(file)(Codec.ISO8859)
      val buf = src.map(_.toByte).toArray
      src.close

      //createRequest(buf, dest, ct) map ({ connection ! _})
      val chunker = chunkActor(connection)
      chunker ! S3ChunkedStart(dest, ct, buf.length)
      chunker ! S3ChunkedData(buf)
      chunker ! S3ChunkedEnd
    case x => log.info("Connected: unknown msg " + x.toString)
  }
}


