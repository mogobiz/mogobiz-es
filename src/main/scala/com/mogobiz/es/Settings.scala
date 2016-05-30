/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.es

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

object Settings extends LazyLogging {
  private val config = ConfigFactory.load("elasticsearch").withFallback(ConfigFactory.load("default-elasticsearch"))

  object ElasticSearch {
    val DateFormat = config.getString("elasticsearch.date.format")
    val Host = config.getString("elasticsearch.host")
    val HttpPort = config.getInt("elasticsearch.http.port")
    val Port = config.getInt("elasticsearch.port")
    val Cluster = config.getString("elasticsearch.cluster")
    val FullUrl = s"$Host:$HttpPort"
    val EsDebug = config.getBoolean("elasticsearch.debug")
    val bulkSize = config.getInt("elasticsearch.bulkSize")
    logger.info(s"ElascticSearch on $Host:$Port, cluster->$Cluster")
  }

}
