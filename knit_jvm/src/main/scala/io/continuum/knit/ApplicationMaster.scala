package io.continuum.knit

import java.io.File
import java.util.Collections
import java.net._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.{AMRMClient, NMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException

import io.continuum.knit.Utils._
import io.continuum.knit.ApplicationMasterArguments.{parseArgs}


import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}

object ApplicationMaster {


  def main(args: Array[String]) {

    implicit val conf = new YarnConfiguration()
    val fs = FileSystem.get(conf)

    val parsedArgs = parseArgs(args)
    println(parsedArgs)

    val pythonEnv = parsedArgs.pythonEnv
    val numContainers = parsedArgs.numContainers
    val shellCMD = parsedArgs.command
    val vCores = parsedArgs.virtualCores
    val mem = parsedArgs.memory
    val files = parsedArgs.files


    val stagingDir = ".knitDeps"
    val user = sys.env.get("KNIT_USER")
    val stagingDirPath = new Path(System.getenv("KNIT_YARN_STAGING_DIR"))

    println(s"User: $user StagingDir: $stagingDirPath")
    println("Running command in container: " + shellCMD)
    val cmd = List(
      shellCMD +
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    )
    println(cmd)

    // Create a client to talk to the RM
    val rmClient = AMRMClient.createAMRMClient().asInstanceOf[AMRMClient[ContainerRequest]]
    rmClient.init(conf)
    rmClient.start()
    val amRMResponse = rmClient.registerApplicationMaster("", 0, "")

    val maxMem = amRMResponse.getMaximumResourceCapability.getMemory
    val maxCores = amRMResponse.getMaximumResourceCapability.getVirtualCores

    println(s"Max memory: $maxMem, Max vCores: $maxCores")

    //create a client to talk to NM
    val nmClient = NMClient.createNMClient()
    nmClient.init(conf)
    nmClient.start()

    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)

    //resources needed by each container
    val resource = Records.newRecord(classOf[Resource])
    println(s"Request: $mem MB of RAM and $vCores number of coress ")
    resource.setMemory(mem)
    resource.setVirtualCores(vCores)

    //request for containers
    for ( i <- 1 to numContainers) {
      val containerAsk = new ContainerRequest(resource,null,null,priority)
      println(s"Requesting Container: $i with Resources: $resource")
      println(s"Requested container ask: $containerAsk")
      rmClient.addContainerRequest(containerAsk)
    }

    var responseId = 0
    var completedContainers = 0
    try {
      while (completedContainers < numContainers) {

        val localResources = HashMap[String, LocalResource]()
        val env = collection.mutable.Map[String, String]()

        //setup local resources
        if (files.nonEmpty) {
          for (file <- files) {
            val name = file.getName
            println(s"Pulling File: $name")
            val localfile = Records.newRecord(classOf[LocalResource])
            val HDFS_FILE_PATH = new Path(stagingDirPath, name).makeQualified(fs.getUri, fs.getWorkingDirectory)
            setUpLocalResource(HDFS_FILE_PATH, localfile)
            localResources(name) = localfile
          }
        }

        //setup python resources
        if (pythonEnv.nonEmpty) {
          val appMasterPython = Records.newRecord(classOf[LocalResource])
          val envFile = new java.io.File(pythonEnv)
          val envZip = envFile.getName
          val envName = envZip.split('.').init(0)
          val PYTHON_ZIP = new Path(stagingDirPath, envZip).makeQualified(fs.getUri, fs.getWorkingDirectory)
          setUpLocalResource(PYTHON_ZIP, appMasterPython, archived = true)
          // set up local ENV
          env("PYTHON_BIN") = s"./PYTHON_DIR/$envName/bin/python"
          env("CONDA_PREFIX") = s"./PYTHON_DIR/$envName/"
          localResources("PYTHON_DIR") = appMasterPython
        }

        setUpEnv(env)

        val response = rmClient.allocate(0.1f)
        responseId += 1
        for (container <- response.getAllocatedContainers.asScala) {
          val ctx =
            Records.newRecord(classOf[ContainerLaunchContext])
          ctx.setCommands(
            List(
              shellCMD +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
            ).asJava
          )

          ctx.setLocalResources(localResources.asJava)
          ctx.setEnvironment(env.asJava)

          System.out.println("Launching container " + container)
          try {
            nmClient.startContainer(container, ctx)
          } catch {
            case e: Exception => println(s"Exception $e")
          }
        }

        for (status <- response.getCompletedContainersStatuses.asScala) {
          println("completed" + status.getContainerId)
          completedContainers += 1

        }

        Thread.sleep(1000)
      }
    } catch {
      case e: org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException =>
        println(s"Yarn Application is possibly being killed: $e")
    }


    rmClient.unregisterApplicationMaster(
      FinalApplicationStatus.SUCCEEDED, "", "")
    rmClient.stop()

  }



}
