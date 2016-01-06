package com.continuumio.rambling

import org.slf4j.Logger
import org.slf4j.LoggerFactory

trait Logging {
  lazy val logger = LoggerFactory.getLogger(getClass)
  implicit def logging2Logger(anything: Logging): Logger = anything.logger
}
