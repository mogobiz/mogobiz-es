package com.mogobiz.es

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load("elasticsearch")

  object ElasticSearch {
    val DateFormat = config.getString("elasticsearch.date.format")
    val Host = config.getString("elasticsearch.host")
    val HttpPort = config.getInt("elasticsearch.http.port")
    val Port = config.getInt("elasticsearch.port")
    val Cluster = config.getString("elasticsearch.cluster")
    val FullUrl = s"$Host:$HttpPort"
    println("ElascticSearch on " + Host + ":" + Port + ", cluster->" + Cluster)
  }

}
