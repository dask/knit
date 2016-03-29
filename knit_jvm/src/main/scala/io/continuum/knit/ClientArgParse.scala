package io.continuum.knit
import java.io.File

import scala.collection.mutable.ArraySeq

import scopt._

case class ClientConfig(callbackHost: String = "127.0.0.1", callbackPort: Int = 0)

object ClientArguments {
  val parser = new scopt.OptionParser[ClientConfig]("scopt") {
    head("knit", "x.1")
    opt[String]('h', "callbackHost") action { (x, c) =>
      c.copy(callbackHost = x)
    } text ("IP of the python callbackhost")

    opt[Int]('p', "callbackPort") action { (x, c) =>
      c.copy(callbackPort = x)
    } text ("Port of the python callbackhost")

    help("help") text ("command line for launching distributed python")

  }

  def parseArgs(args: Array[String]) : ClientConfig = {
    val parsed = parser.parse(args, ClientConfig())
    val parsedArgs = parsed.getOrElse( sys.exit(1) )
    parsedArgs
  }
}