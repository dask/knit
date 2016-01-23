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


    val stagingDir = ".knitDeps"
    val stagingDirPath = new Path(fs.getHomeDirectory(), stagingDir)

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
    rmClient.registerApplicationMaster("", 0, "")


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
      println("asking for " +s"$i")
      rmClient.addContainerRequest(containerAsk)
    }

    var responseId = 0
    var completedContainers = 0

    while( completedContainers < numContainers) {

      val localResources = HashMap[String, LocalResource]()
      val env = collection.mutable.Map[String,String]()


      //setup local resources
      if (!pythonEnv.isEmpty) {
        val appMasterPython = Records.newRecord(classOf[LocalResource])
        val PYTHON_ZIP = new Path(stagingDirPath, "miniconda-env.zip").makeQualified(fs.getUri, fs.getWorkingDirectory)
        setUpLocalResource(PYTHON_ZIP, appMasterPython, archived = true)
        // set up local ENV
        env("PYTHON_BIN") = "./PYTHON_DIR/miniconda-env/bin/python"
        env("CONDA_PREFIX") = "./PYTHON_DIR/miniconda-env/"

        localResources("PYTHON_DIR") = appMasterPython
        localResources("PYTHON_DIR3") = appMasterPython

      }

      setUpEnv(env)

      val response = rmClient.allocate(responseId+1)
      responseId+=1
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
        nmClient.startContainer(container, ctx)
      }

      for ( status <- response.getCompletedContainersStatuses.asScala){
        println("completed"+status.getContainerId)
        completedContainers+=1

      }

      Thread.sleep(10000)
    }

    rmClient.unregisterApplicationMaster(
      FinalApplicationStatus.SUCCEEDED, "", "")
  }



}
