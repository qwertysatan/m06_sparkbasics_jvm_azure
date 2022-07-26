package utils

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

object ConfigUtils {

  private val CONF_FILE_NAME = "application.conf"

  lazy val conf = ConfigFactory.parseFile(new File(CONF_FILE_NAME)).withFallback(ConfigFactory.load())


}
