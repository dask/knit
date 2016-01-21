package com.continuumio.rambling

import java.util.Collections
import java.net._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.{AMRMClient, NMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import Utils._

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}

object ApplicationMaster {


  def main(args: Array[String]) {
    val n = args(1).toInt
    val shellCMD = args(2)
    val vCores = args(3).toInt
    val mem = args(4).toInt

    println("Running command in container: " + shellCMD)
    val cmd = List(
      shellCMD +
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    )
    println(cmd)

    implicit val conf = new YarnConfiguration()

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
    resource.setMemory(mem)
    resource.setVirtualCores(vCores)


    //request for containers
    for ( i <- 1 to n) {
      val containerAsk = new ContainerRequest(resource,null,null,priority)
      println("asking for " +s"$i")
      rmClient.addContainerRequest(containerAsk)
    }

    var responseId = 0
    var completedContainers = 0

    while( completedContainers < n) {

      //setup local resources
      val appMasterPython = Records.newRecord(classOf[LocalResource])
      setUpLocalResource(new Path("/jars/miniconda-env.zip"),appMasterPython, archived = true)

      //set up local ENV
      val env = collection.mutable.Map[String,String]()
      env("PYTHON_BIN") = "./PYTHON_DIR/miniconda-env/bin/python"
      env("CONDA_PREFIX") = "./PYTHON_DIR/miniconda-env/"
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

        //setup local resources
        val localResources = HashMap[String, LocalResource]()
        localResources("PYTHON_DIR") = appMasterPython
        localResources("PYTHON_DIR3") = appMasterPython
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
