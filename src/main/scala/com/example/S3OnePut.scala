package com.example

import akka.actor.{Actor, ActorLogging}

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 31/01/2014
 */

object S3OnePut extends S3Put {
  def apply(bucket: String, key: String, secret: String) =
    new S3OnePut(bucket, key,  secret)

}

class S3OnePut(bucket: String, key: String, secret: String)
  extends S3Put with Actor with ActorLogging {

}
