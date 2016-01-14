package com.continuumio.rambling

import java.util.Collections

import com.continuumio.rambling.Utils._
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

import scala.collection.JavaConverters._

/**
 * Entry point into the Yarn application.
 *
 * This is a YARN client which launches an Application master by adding the jar to local resources
 *
 */
object Client extends Logging {

  def main(args: Array[String]) {

    logger.error("Staring Application Master")

    implicit val conf = new YarnConfiguration()
    val jarPath = args(0)
    val numberOfInstances = args(1).toInt
    val shellCMD = "\\\""+args(2)+"\\\""
    val vCores = args(3).toInt
    val mem = args(4).toInt

    logger.info("Running commmand: " + shellCMD)

    //    val fs = FileSystem.get(conf);
//    fs.copyFromLocalFile(new Path("/vagrant/rambling-1.0-SNAPSHOT.jar"), new Path("/tmp/rambling-1.0-SNAPSHOT.jar"));

    // start a yarn client
    val client = YarnClient.createYarnClient()
    client.init(conf)
    client.start()

    // application creation
    val app = client.createApplication()
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    //application master is a just java program with given commands
    amContainer.setCommands(List(
      "$JAVA_HOME/bin/java" +
        " -Xmx256M" +
        " com.continuumio.rambling.ApplicationMaster" +
        "  "  + jarPath +"   " + numberOfInstances + "  " + shellCMD + " " + vCores + " " + mem + " " +
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    ).asJava)


    //add the jar which contains the Application master code to classpath
    val appMasterJar = Records.newRecord(classOf[LocalResource])
    setUpLocalResource(new Path(jarPath), appMasterJar)
    amContainer.setLocalResources(Collections.singletonMap("rambling.jar", appMasterJar))

    //setup env to get all yarn and hadoop classes in classpath
    val env = collection.mutable.Map[String, String]()
    setUpEnv(env)
    amContainer.setEnvironment(env.asJava)

    //specify resource requirements
    val resource = Records.newRecord(classOf[Resource])

    //The unit for memory is megabytes
    resource.setMemory(300)
    resource.setVirtualCores(1)

    //context to launch
    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName("rambling")
    appContext.setAMContainerSpec(amContainer)
    appContext.setResource(resource)
    appContext.setQueue("default")

    //submit the application
    val appId = appContext.getApplicationId
    println("submitting application id" + appId)
    client.submitApplication(appContext)

  }

}
