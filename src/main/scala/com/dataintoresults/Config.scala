package com.dataintoresults

import com.typesafe.config.ConfigFactory

object Config {
  lazy val config = {
    val baseConfig = ConfigFactory.load()
    if(baseConfig.hasPath("environment")) {
      val env = baseConfig.getString("environment")
      baseConfig.withFallback(baseConfig.getConfig(env))
    }
    else  
      baseConfig      
  }
}