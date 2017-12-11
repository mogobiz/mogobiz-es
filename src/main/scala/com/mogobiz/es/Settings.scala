/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.es

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

object Settings {
  val logger         = Logger(LoggerFactory.getLogger("com.mogobiz.es.ElasticSearch"))
  private val config = ConfigFactory.load("elasticsearch").withFallback(ConfigFactory.load("default-elasticsearch"))

  object ElasticSearch {
    val DateFormat: String = config.getString("elasticsearch.date.format")
    val HttpHost: String   = config.getString("elasticsearch.http.host")
    val Cluster: String    = config.getString("elasticsearch.cluster")
    val FullUrl: String    = s"elasticsearch://$HttpHost?cluster.name=$Cluster"
    val EsDebug: Boolean   = config.getBoolean("elasticsearch.debug")
    val bulkSize: Int      = config.getInt("elasticsearch.bulkSize")
    logger.info(s"ElascticSearch on $FullUrl")
  }

}
