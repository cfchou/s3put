package com.example

import com.typesafe.config.ConfigFactory
import akka.actor.{Props, ActorSystem}
import akka.pattern.{ask, pipe}
import scala.io.{Codec, Source}
import scala.concurrent.{ExecutionContext, Future}
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps
import com.example.S3Put.{S3CommandResult, S3Connected, S3Connect}
import com.example.S3StreamPut.{S3ChunkedEnd, S3ChunkedData, S3ChunkedStart}
import scala.util.{Success, Failure}

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 09/02/2014
 */
object S3SerialClient extends App {
  val appConf = ConfigFactory.load
  val bucket = appConf getString "s3.bucket"
  val key = appConf getString "aws.key"
  val secret = appConf getString "aws.secret"

  val file = appConf getString "uploadTest.object"
  val objectId = appConf getString "uploadTest.objectId"

  val chunkSize = 1024 * 10
  val buf = {
    val src = Source.fromFile(file)(Codec.ISO8859)
    val buf = src.map(_.toByte).toArray
    src.close
    buf
  }

  val system = ActorSystem("S3SerialClient")
  implicit val timeout = Timeout(10 seconds)
  import ExecutionContext.Implicits.global

  val s3put = system.actorOf(Props(S3StreamPutFSM(bucket, key, secret)),
    "s3StreamPutFSM")

  def agenda: Future[_] = {
    def transformHelper(f: Future[_], msg: String): Future[_] = {
      f.transform({ x: Any => println(s"$msg: $x"); x},
      { e: Throwable => println(s"$msg: $e"); e })
    }
    val init = transformHelper(s3put ? S3Connect, "** Conn **") flatMap { _ =>
      transformHelper(s3put ? S3ChunkedStart(objectId, None, buf.length),
        "** Start **")
    }
    def appendChunk(run: Future[_], data: Array[Byte]): Future[_] = {
        if (data.length == 0) {
          val f = run flatMap { _ => s3put ? S3ChunkedEnd }
          transformHelper(f, "** End **")
        } else if (data.length > chunkSize) {
          val f = run flatMap { _ =>
            s3put ? S3ChunkedData(data.take(chunkSize)) }
          appendChunk(transformHelper(f, "** Chk **"), data.drop(chunkSize))
        } else {
          val f = run flatMap { _ =>
            s3put ? S3ChunkedData(data)
          }
          appendChunk(transformHelper(f, "** Chk **"), Array.empty)
        }
    }
    appendChunk(init, buf)
  }

  agenda onComplete({
    case Success(v) =>
      println(s"Success: $v")
    case Failure(e) =>
      println(s"Failure: $e")
  })

  import ExecutionContext.Implicits.global
  system.scheduler.scheduleOnce(10.seconds)({
    println("Shutdown actors")
    system.shutdown()
  })
}
