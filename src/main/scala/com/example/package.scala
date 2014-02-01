package com

import com.typesafe.config.ConfigFactory
import scala.util.Try

/**
 * Created with IntelliJ IDEA.
 * User: cfchou
 * Date: 01/02/2014
 */
package object example {

  val appConf = Try({ConfigFactory.load("application")})

}
