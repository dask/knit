package io.continuum.knit
import java.io.File

import io.continuum.knit.Utils._
import io.continuum.knit.ClientArguments.parseArgs

import java.net._
import java.util.Collections
import java.io.DataOutputStream
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}

import py4j.GatewayServer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
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
  
  var client: YarnClient = _
  var appId: ApplicationId = _

  def main(args: Array[String]) {
    //from https://github.com/apache/spark/blob/d83c2f9f0b08d6d5d369d9fae04cdb15448e7f0d/core/src/main/scala/org/apache/spark/api/python/PythonGatewayServer.scala
    //thank you spark
    
    //Start a GatewayServer on an ephemeral port
    val gatewayServer: GatewayServer = new GatewayServer(this, 0)
    gatewayServer.start()
    
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      logger.error("GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      logger.debug(s"Started PythonGatewayServer on port $boundPort")
    }
    
    val parsedArgs = parseArgs(args)
    logger.debug(f"$parsedArgs%s")
    
    // Communicate the bound port back to the caller via the caller-specified callback port
    val callbackHost = parsedArgs.callbackHost
    val callbackPort = parsedArgs.callbackPort
    
    logger.debug(s"Communicating GatewayServer port to Python driver at $callbackHost:$callbackPort")
    val callbackSocket = new Socket(callbackHost, callbackPort)
    val dos = new DataOutputStream(callbackSocket.getOutputStream)
    dos.writeInt(boundPort)
    dos.close()
    callbackSocket.close()

    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while (System.in.read() != -1) {
      // Do nothing
    }
    logger.debug("Exiting due to broken pipe from Python driver")
    System.exit(0)
  }
  
  def start(pythonEnv: String, files: String, appName: String, queue: String, upload: String) : String = {
    logger.info("Starting Application Master")

    implicit val conf = new YarnConfiguration()
    val fs = FileSystem.get(conf)
    val cred = new Credentials()
    val out = fs.addDelegationTokens("yarn", cred)

    setDependencies()

    val stagingDir = ".knitDeps"
    val stagingDirPath = new Path(sys.env("HDFS_KNIT_DIR"), stagingDir)
    val KNIT_JAR = new Path(stagingDirPath, "knit-1.0-SNAPSHOT.jar")
    val KNIT_JAR_PATH = KNIT_JAR.makeQualified(fs.getUri, fs.getWorkingDirectory)
    logger.debug(f"$KNIT_JAR_PATH%s")

    // start a yarn client
    client = YarnClient.createYarnClient()
    client.init(conf)
    client.start()

    // application creation
    val app = client.createApplication()
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])

    val dob = new DataOutputBuffer()
    cred.writeTokenStorageToStream(dob)
    amContainer.setTokens(ByteBuffer.wrap(dob.getData()))

    //add the jar which contains the Application master code to classpath
    val appMasterJar = Records.newRecord(classOf[LocalResource])
    setUpLocalResource(KNIT_JAR_PATH, appMasterJar)

    val localResources = HashMap[String, LocalResource]()
    //setup env to get all yarn and hadoop classes in classpath
    val env = collection.mutable.Map[String, String]()
    env("KNIT_USER") = UserGroupInformation.getCurrentUser.getShortUserName
    env("KNIT_YARN_STAGING_DIR") = stagingDirPath.toString
    
    if (files.length > 0) {
      val fileArray = files.split(",")
      for (fileName <- fileArray) {
        val file = new File(fileName)
        uploadFile(file.getAbsolutePath)  
        val name = file.getName  

        val fileUpload = Records.newRecord(classOf[LocalResource])
        val HDFS_FILE_UPLOAD = new Path(stagingDirPath, name)
        setUpLocalResource(HDFS_FILE_UPLOAD.makeQualified(fs.getUri, fs.getWorkingDirectory), fileUpload)
      }
    }

    if (pythonEnv.nonEmpty) {
      //TODO: detect file suffix
      if (upload == "True"){
        uploadFile(pythonEnv)
      } else {
        logger.debug("Using cached environment")
      }
      val envFile = new java.io.File(pythonEnv)
      val envZip = envFile.getName
      val envName = envZip.split('.').init(0)
      val appMasterPython = Records.newRecord(classOf[LocalResource])
      val PYTHON_ZIP = new Path(stagingDirPath, envZip)
      setUpLocalResource(PYTHON_ZIP.makeQualified(fs.getUri, fs.getWorkingDirectory), appMasterPython, archived = true)
      localResources("PYTHON_DIR") = appMasterPython
      env("PYTHON_BIN") = s"./PYTHON_DIR/$envName/bin/python"
      env("CONDA_PREFIX") = s"./PYTHON_DIR/$envName/"
    }

    //add the jar which contains the Application master code to classpath
    localResources("knit.jar") = appMasterJar

    amContainer.setLocalResources(localResources.asJava)

    setUpEnv(env)
    amContainer.setEnvironment(env.asJava)

    //application master is a just java program with given commands
    amContainer.setCommands(List(
      "$JAVA_HOME/bin/java" +
        " -Xmx256M" +
        " io.continuum.knit.ApplicationMaster" +
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
    appContext.setApplicationName(appName)
    appContext.setAMContainerSpec(amContainer)
    appContext.setResource(resource)
    appContext.setQueue(queue)

    //submit the application
    appId = appContext.getApplicationId
    logger.info(s"Submitting application $appId")
    client.submitApplication(appContext)
    
    return appId.toString
  }
  
  def masterRPCHost(): String = {
    val appReport = client.getApplicationReport(appId)
    appReport.getHost
  }

  def getContainers(): String = {
    val attempts = client.getApplicationAttempts(appId).asScala
    val attempt = attempts.last

    logger.info(s"Getting containers for $attempt")
    val containers = client.getContainers(attempt.getApplicationAttemptId).asScala
    var list_ = List[String]()

    for (container <- containers) {
      list_ ::= container.getContainerId.toString
    }

    val container_list = list_.mkString(",")
    logger.info(s"Container ID: $container_list")
    container_list
  }

  def masterRPCPort(): Int = {
    val appReport = client.getApplicationReport(appId)
    appReport.getRpcPort
  }

  def numUsedContainers() : Int = {
    val appReport = client.getApplicationReport(appId)
    val usageReport = appReport.getApplicationResourceUsageReport
    usageReport.getNumUsedContainers
  }

  def status() : String = {
    val appReport = client.getApplicationReport(appId)
    appReport.getYarnApplicationState.name
  }

  def applicationAttempts() : String = {
    val attempts = client.getApplicationAttempts(appId)
    attempts.toString
  }

  def kill() : Boolean = {
    client.killApplication(appId)
    true
  }

  def exit() {
    logger.info("Client exit")
    System.exit(0)
  }
  
}
