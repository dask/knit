package com.continuumio.rambling

import java.io.File
import scopt._

case class Config(numInstances: Int = 1, memory: Int = 300, virutalCores: Int = 1,
                  command: String = "", jarPath: String = "", debug: Boolean = false)

object ClientArguments {
  val parser = new scopt.OptionParser[Config]("scopt") {
    head("rambling", "x.1")
    opt[Int]('n', "numInstances") action { (x, c) =>
      c.copy(numInstances = x)
    } text ("Number of YARN containers")

    opt[Int]('m', "memory") action { (x, c) =>
      c.copy(memory = x)
    } text ("Amount of memory per container")

    opt[Int]('c', "virutalCores") action { (x, c) =>
      c.copy(virutalCores = x)
    } text ("Virtual cores per container")

    opt[String]('C', "command") action { (x, c) =>
      c.copy(command = x)
    } text ("Command to run in containers")

    opt[String]('j', "jarPath") action { (x, c) =>
      c.copy(jarPath = x)
    } text ("Number of YARN containers ")

    opt[Unit]("debug") hidden() action { (_, c) =>
      c.copy(debug = true)
    } text ("this option is hidden in the usage text")

    help("help") text ("command line for launching distributed python")

  }

  def parseArgs(args: Array[String]) : Config = {
    val parsed = parser.parse(args, Config())
    val parsedArgs = parsed.getOrElse( sys.exit(1) )
    parsedArgs
  }
}