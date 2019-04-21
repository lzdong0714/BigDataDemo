package com.demo.data

import org.slf4j
import org.slf4j.LoggerFactory

object MainDemo{
  val logger: slf4j.Logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    logger.info("start")
    Submit.parser parse(args, Submit.Config()) match {
      case Some(config) =>
        operation(config)
      case _ => throw new IllegalArgumentException("Error parsing command line arguments.")
    }
  }
}
