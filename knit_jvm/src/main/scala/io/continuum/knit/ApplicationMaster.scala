package io.continuum.knit

import java.io._
import java.net._
import java.nio.ByteBuffer
import java.util.Collections
import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._ 
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.log4j.Logger

import io.continuum.knit.Utils._

import py4j.GatewayServer

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}

object ApplicationMaster extends Logging with AMRMClientAsync.CallbackHandler with NMClientAsync.CallbackHandler {
  
  var gatewayServer: GatewayServer = _
  
  implicit var conf: YarnConfiguration = _
  var fs: FileSystem = _
  var rmClient: AMRMClientAsync[ContainerRequest] = _
  var nmClient: NMClientAsync = _

  var registerResponse : RegisterApplicationMasterResponse = _
  var outstandingRequests = List[ContainerRequest]()
  var cred: Credentials = _
  
  var files: String = _
  var pythonEnv: String = _
  var shellCMD: String = _
  
  var numRequested = 0
  var numCompleted = 0
  var numFailed = 0
  var done = false

  def main(args: Array[String]) {
    logger.info("Construct application master")
    
    val localInetAddress = findLocalInetAddress()
    gatewayServer = new GatewayServer(this, 0, 
        GatewayServer.DEFAULT_PYTHON_PORT,
        localInetAddress,
        localInetAddress,
        GatewayServer.DEFAULT_CONNECT_TIMEOUT,
        GatewayServer.DEFAULT_READ_TIMEOUT,
        null)
    
    gatewayServer.start()
    
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      logger.error("GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      logger.debug(s"Started PythonGatewayServer on port $boundPort")
    }
    
    conf = new YarnConfiguration()
    fs = FileSystem.get(conf)
    UserGroupInformation.isSecurityEnabled()
    val creds = UserGroupInformation.getCurrentUser().getCredentials()
    cred = new Credentials(creds)
    val nots = creds.numberOfTokens()
    logger.info(f"Number of tokens: $nots")
    for (tok <- creds.getAllSecretKeys()) {
        logger.info(f"TOKEN: $tok")
    }

    // Create a client to talk to the RM
    rmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](1000, this)
    rmClient.init(conf)
    rmClient.start()
    registerResponse = rmClient.registerApplicationMaster(localInetAddress.getHostAddress, boundPort, "")
    
    //create a client to talk to NM
    nmClient = NMClientAsync.createNMClientAsync(this)
    nmClient.init(conf)
    nmClient.start()
  }
  
  def init(pythonEnv: String, files: String, shellCMD: String, numContainers: Int, vCores: Int, mem: Int) {
    logger.info("Init application master")
    
    this.pythonEnv = pythonEnv
    this.files = files
    this.shellCMD = shellCMD
    
    val previousAttempts = registerResponse.getContainersFromPreviousAttempts
    val nrRunningContainers = previousAttempts.size
    val nrContainersToRequest = numContainers - nrRunningContainers
    
    logger.info(s"$nrRunningContainers containers still running, requesting $nrContainersToRequest")
    addContainers(nrContainersToRequest, vCores, mem)
  }

  def addContainers(numContainers: Int, vCores: Int, mem: Int) {
    logger.info(s"Add $numContainers containers")

    //resources needed by each container
    val resource = Records.newRecord(classOf[Resource])
    logger.info(s"Request: $mem MB of RAM and $vCores cores")
    resource.setMemory(mem)
    resource.setVirtualCores(vCores)
    
    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)

    //request for containers
    for ( i <- 1 to numContainers) {
      val containerAsk = new ContainerRequest(resource,null,null,priority)
      logger.info(s"Requested container resources: $resource")
      logger.info(s"Requested container ask: $containerAsk")
      
      rmClient.addContainerRequest(containerAsk)
      outstandingRequests ::= containerAsk
      numRequested += 1
    }
  }

  def removeContainer(_containerId: String) {
    logger.info("Releasing Container: $_containerId")
    val containerId = ContainerId.fromString(_containerId)
    rmClient.releaseAssignedContainer(containerId)
  }

  override def onContainersAllocated(containers: java.util.List[Container]) = {
    val stagingDir = ".knitDeps"
    val user = sys.env.get("KNIT_USER")
    val stagingDirPath = new Path(System.getenv("KNIT_YARN_STAGING_DIR"))

    logger.debug(s"User: $user StagingDir: $stagingDirPath")
    logger.info(s"Running command in container: $shellCMD")

    //containers allocated, first remove all requests    
    for (container <- containers.asScala) {
      if (outstandingRequests.isEmpty) {
        //not expecting this container, releasing
        //see https://issues.apache.org/jira/browse/YARN-1902 
        //and https://issues.apache.org/jira/browse/YARN-3020
        rmClient.releaseAssignedContainer(container.getId)
        
      } else {
        rmClient.removeContainerRequest(outstandingRequests.last)
        outstandingRequests = outstandingRequests.init

        val localResources = HashMap[String, LocalResource]()
        val env = collection.mutable.Map[String, String]()
  
        //setup local resources
        if (files.length > 0) {
          val fileArray = files.split(",")
          for (fileName <- fileArray) {
            val file = new File(fileName)
            val name = file.getName
    
            logger.debug(s"Pulling File: $name")
            val localfile = Records.newRecord(classOf[LocalResource])
            val HDFS_FILE_PATH = new Path(stagingDirPath, name).makeQualified(fs.getUri, fs.getWorkingDirectory)
            setUpLocalResource(HDFS_FILE_PATH, localfile)
            localResources(name) = localfile
          }
        }
  
        //setup python resources
        if (pythonEnv.nonEmpty) {
          val appMasterPython = Records.newRecord(classOf[LocalResource])
          val envFile = new File(pythonEnv)
          val envZip = envFile.getName
          val envName = envZip.split('.').init(0)
          val PYTHON_ZIP = new Path(stagingDirPath, envZip).makeQualified(fs.getUri, fs.getWorkingDirectory)
          setUpLocalResource(PYTHON_ZIP, appMasterPython, archived = true)
          // set up local ENV
          env("PYTHON_BIN") = s"./PYTHON_DIR/$envName/bin/python"
          env("CONDA_PREFIX") = s"./PYTHON_DIR/$envName/"
          env("LC_ALL") = "C.UTF-8"
          localResources("PYTHON_DIR") = appMasterPython
        }
  
        setUpEnv(env)
        
        val ctx = Records.newRecord(classOf[ContainerLaunchContext])
        ctx.setCommands(
          List(
            shellCMD +
              " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
              " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
          ).asJava
        )

        val dob = new DataOutputBuffer()
        for (tok <- cred.getAllTokens()) {
            logger.info(f"TOKEN ADDED: $tok")
        }

        cred.writeTokenStorageToStream(dob)
        ctx.setTokens(ByteBuffer.wrap(dob.getData()))

        ctx.setLocalResources(localResources.asJava)
        ctx.setEnvironment(env.asJava)
    
        logger.info(s"Launching container $container")
        try {
          nmClient.startContainerAsync(container, ctx)
        } catch {
          case e: Exception => 
            logger.error("Exception", e)
        }
      }
    }
  }
  
  override def onContainersCompleted(statuses: java.util.List[ContainerStatus]) = {
    for (status <- statuses.asScala) {
        numCompleted += 1
        
        logger.info(s"Container completed $status.getContainerId")
        if (status.getExitStatus != 0) {
          numFailed += 1
        }
    }

    if (numCompleted >= numRequested) {
      nmClient.stop()
      
      try {
        if (numFailed > 0) {
          rmClient.unregisterApplicationMaster(FinalApplicationStatus.FAILED, "", "")
        } else {
          rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "")
        }
      } catch {
        case e: Exception => 
          logger.error("Exception", e)
      }
      
      rmClient.stop()
      
      gatewayServer.shutdown()
      
      done = true
    }
  }
  
  override def onNodesUpdated(updatedNodes: java.util.List[NodeReport]) = {
    
  }
  
  override def onShutdownRequest = {
    logger.info("Got shutdown request")
  }
  
  override def onError(e: Throwable) = {
    logger.error("Received error", e)
  }
  
  override def getProgress: Float = {
    if (numRequested > 0) {
      0.1F + (numCompleted/(numRequested * 0.9F))
    } else {
      0.1F
    }
  }
  
  override def onContainerStarted(containerId: ContainerId, allServiceResponse: java.util.Map[String, ByteBuffer]) = {
    logger.info(s"Container started $containerId")
  }
  override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus) = {
    logger.debug(s"Container status $containerId: $containerStatus")
  }
  override def onContainerStopped(containerId: ContainerId) = {
    logger.info(s"Container stopped $containerId")
  }
  override def onStartContainerError(containerId: ContainerId, t: Throwable) = {
    logger.error(s"Container start error $containerId", t)
  }
  override def onGetContainerStatusError(containerId: ContainerId, t: Throwable) = {
    logger.error(s"Container status error $containerId", t)
  }
  override def onStopContainerError(containerId: ContainerId, t: Throwable) = {
    logger.error(s"Container stop error $containerId", t)
  }
  
  def getNumRequested() = {
    numRequested
  }
  def getNumCompleted() = {
    numCompleted
  }
  def getNumFailed() = {
    numFailed
  }


  //from https://raw.githubusercontent.com/apache/spark/e97fc7f176f8bf501c9b3afd8410014e3b0e1602/core/src/main/scala/org/apache/spark/util/Utils.scala
  //thank you spark
  private def findLocalInetAddress(): InetAddress = {
    val address = InetAddress.getLocalHost
    if (address.isLoopbackAddress) {
      // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
      // a better address using the local network interfaces
      // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
      // on unix-like system. On windows, it returns in index order.
      // It's more proper to pick ip address following system output order.
      val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
      val reOrderedNetworkIFs = activeNetworkIFs.reverse

      for (ni <- reOrderedNetworkIFs) {
        val addresses = ni.getInetAddresses.asScala
          .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
        if (addresses.nonEmpty) {
          val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
          // because of Inet6Address.toHostName may add interface at the end if it knows about it
          val strippedAddress = InetAddress.getByAddress(addr.getAddress)
          // We've found an address that looks reasonable!
          logger.warn("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
            " a loopback address: " + address.getHostAddress + "; using " +
            strippedAddress.getHostAddress + " instead (on interface " + ni.getName + ")")
          return strippedAddress
        }
      }
      logger.warn("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
        " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
        " external IP address!")
    }
    address
  }
}
