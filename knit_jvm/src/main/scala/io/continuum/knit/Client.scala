package io.continuum.knit
import java.io.File

import io.continuum.knit.Utils._
import io.continuum.knit.ClientArguments.parseArgs

import java.net._
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ContainerLaunchContext, _}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.apache.log4j.Logger


/**
 * Entry point into the Yarn application.
 *
 * This is a YARN client which launches an Application master by adding the jar to local resources
 *
 */
object Client extends Logging {

  def main(args: Array[String]) {
    val parsedArgs = parseArgs(args)
    println(parsedArgs)

    logger.info("Staring Application Master")

    implicit val conf = new YarnConfiguration()
    val fs = FileSystem.get(conf)
    setDependencies()

//    val jarPath = parsedArgs.jarPath
    val numberOfInstances = parsedArgs.numInstances
    val CMD = parsedArgs.command
    val vCores = parsedArgs.virutalCores
    val mem = parsedArgs.memory

    val cleanedCMD = CMD.replace("\"", "\'")
    val shellCMD = "\\\""+cleanedCMD+"\\\""

    logger.info("Running commmand: " + shellCMD)
    val stagingDir = ".knitDeps"
    val stagingDirPath = new Path(fs.getHomeDirectory(), stagingDir)
    val KNIT_JAR = new Path(stagingDirPath, "knit-1.0-SNAPSHOT.jar")
    val KNIT_JAR_PATH = KNIT_JAR.makeQualified(fs.getUri, fs.getWorkingDirectory)
    println(KNIT_JAR_PATH)

    // start a yarn client
    val client = YarnClient.createYarnClient()
    client.init(conf)
    client.start()

    // application creation
    val app = client.createApplication()
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])

    //add the jar which contains the Application master code to classpath
    val appMasterJar = Records.newRecord(classOf[LocalResource])
    setUpLocalResource(KNIT_JAR_PATH, appMasterJar)



    //TODO: move to .knitDeps and allow for users to define zip name and file location
//    val appMasterPython = Records.newRecord(classOf[LocalResource])
//    val PYTHON_ZIP = new Path("/jars/miniconda-env.zip").makeQualified(fs.getUri, fs.getWorkingDirectory)
//    setUpLocalResource(PYTHON_ZIP,appMasterPython, archived = true)

    val localResources = HashMap[String, LocalResource]()
//    localResources("PYTHON_DIR") = appMasterPython
//    localResources("PYTHON_DIR3") = appMasterPython

    //add the jar which contains the Application master code to classpath
    localResources("knit.jar") = appMasterJar


    amContainer.setLocalResources(localResources.asJava)

    //setup env to get all yarn and hadoop classes in classpath
    val env = collection.mutable.Map[String, String]()
    env("PYTHON_BIN") = "./PYTHON_DIR/miniconda-env/bin/python"
    env("CONDA_PREFIX") = "./PYTHON_DIR/miniconda-env/"
    setUpEnv(env)
    amContainer.setEnvironment(env.asJava)

    //application master is a just java program with given commands
    amContainer.setCommands(List(
      "$JAVA_HOME/bin/java" +
        " -Xmx256M" +
        " io.continuum.knit.ApplicationMaster" +
        "  " + numberOfInstances + "  " + shellCMD + " " + vCores + " " + mem + " " +
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    ).asJava)

    //specify resource requirements
    val resource = Records.newRecord(classOf[Resource])

    //The unit for memory is megabytes
    resource.setMemory(300)
    resource.setVirtualCores(1)

    //context to launch
    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName("knit")
    appContext.setAMContainerSpec(amContainer)
    appContext.setResource(resource)
    appContext.setQueue("default")

    //submit the application
    val appId = appContext.getApplicationId
    println("submitting application id" + appId)
    client.submitApplication(appContext)

  }

}
