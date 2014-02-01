package com.example

import akka.event.LoggingAdapter
import scala.concurrent.{ExecutionContext, Future}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import spray.http.{ContentTypes, MediaType, MediaTypes, ContentType}

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 01/02/2014
 */

// TODO: decouple LoggingAdapter
trait S3P { this: { def bucket: String
                    def key: String
                    def secret: String
                    def log: LoggingAdapter
                  } =>
  require(!bucket.isEmpty && !key.isEmpty && !secret.isEmpty)
  val bucketHost = "%s.s3.amazonaws.com" format bucket

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
           amzHeaders: List[String])
          (implicit ec: ExecutionContext): Future[String] = Future {
    val message = (List("PUT", contentMD5.getOrElse(""),
      contentType.getOrElse("application/octet-stream"), date) ++
      canonicalizedAmzHeaders(amzHeaders) :+
      canonicalizedResource(dest)).mkString("\n")
    log.info(message)

    val mac = Mac.getInstance("HmacSHA1")
    mac.init(new SecretKeySpec(secret.getBytes("UTF-8"), "HmacSHA1"))
    new String(Base64.encodeBase64(mac.doFinal(message.getBytes("UTF-8"))))
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
}

