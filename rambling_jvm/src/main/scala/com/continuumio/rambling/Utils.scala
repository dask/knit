package com.continuumio.rambling

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{LocalResourceVisibility, LocalResourceType, LocalResource}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils}
import scala.collection.JavaConverters._


object Utils {

  // add details of the local resource*
  def setUpLocalResource(resourcePath: Path, resource: LocalResource)(implicit conf:Configuration) = {
    val jarStat = FileSystem.get(conf).getFileStatus(resourcePath)
    resource.setResource(ConverterUtils.getYarnUrlFromPath(resourcePath))
    resource.setSize(jarStat.getLen())
    resource.setTimestamp(jarStat.getModificationTime())
    resource.setType(LocalResourceType.FILE)
    resource.setVisibility(LocalResourceVisibility.PUBLIC)
  }

  //add the yarn jars to classpath
  def setUpEnv(env: collection.mutable.Map[String, String])(implicit conf:YarnConfiguration) = {
    val classPath =  conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH:_*)
    for (c <- classPath){
      Apps.addToEnvironment(env.asJava, Environment.CLASSPATH.name(),
        c.trim())
    }
    Apps.addToEnvironment(env.asJava,
      Environment.CLASSPATH.name(),
      Environment.PWD.$() + File.separator + "*")

  }



}
