package com.mogobiz.es

import java.util.{Calendar, Date}

import com.mogobiz.json.JacksonConverter
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.source.DocumentSource
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.search.SearchHit

object EsClient {
  val settings = ImmutableSettings.settingsBuilder().put("cluster.name", Settings.ElasticSearch.Cluster).build()
  val client = ElasticClient.remote(settings, (Settings.ElasticSearch.Host, Settings.ElasticSearch.Port))

  def apply() = {
    client.sync
  }

  type Timestamped = {
    val uuid: String
    var lastUpdated: Date
    var dateCreated: Date
  }

  def index[T <: Timestamped : Manifest](indexName:String, t: T, refresh: Boolean): String = {
    val now = Calendar.getInstance().getTime
    t.dateCreated = now
    t.lastUpdated = now
    val json = JacksonConverter.serialize(t)
    val res = client.client.prepareIndex(indexName, manifest[T].runtimeClass.getSimpleName, t.uuid)
      .setSource(json)
      .setRefresh(refresh)
      .execute()
      .actionGet()
    res.getId
  }

  def load[T: Manifest](indexName:String, uuid: String): Option[T] = {
    load[T](indexName, uuid, manifest[T].runtimeClass.getSimpleName)
  }

  def load[T: Manifest](indexName:String, uuid: String, esDocumentName:String): Option[T] = {
    val req = get id uuid from indexName -> esDocumentName
    val res = client.sync.execute(req)
    if (res.isExists) Some(JacksonConverter.deserialize[T](res.getSourceAsString)) else None
  }

  def loadWithVersion[T: Manifest](indexName:String, uuid: String): Option[(T, Long)] = {
    val req = get id uuid from indexName -> manifest[T].runtimeClass.getSimpleName
    val res = client.sync.execute(req)
    val maybeT = if (res.isExists) Some(JacksonConverter.deserialize[T](res.getSourceAsString)) else None
    maybeT map ((_, res.getVersion))
  }

  def delete[T: Manifest](indexName:String, uuid: String, refresh: Boolean): Boolean = {
    val req = com.sksamuel.elastic4s.ElasticDsl.delete id uuid from indexName -> manifest[T].runtimeClass.getSimpleName refresh refresh
    val res = client.sync.execute(req)
    res.isFound
  }

  def update[T <: Timestamped : Manifest](indexName:String, t: T, upsert: Boolean, refresh: Boolean): Boolean = {
    update[T](indexName, t, manifest[T].runtimeClass.getSimpleName, upsert, refresh)
  }

  def update[T <: Timestamped : Manifest](indexName:String, t: T, esDocumentName:String, upsert: Boolean, refresh: Boolean): Boolean = {
    val now = Calendar.getInstance().getTime
    t.lastUpdated = now
    val js = JacksonConverter.serialize(t)
    val req = com.sksamuel.elastic4s.ElasticDsl.update id t.uuid in indexName -> esDocumentName refresh refresh doc new DocumentSource {
      override def json: String = js
    }
    req.docAsUpsert(upsert)
    val res = client.sync.execute(req)
    res.isCreated || res.getVersion > 1
  }

  def update[T <: Timestamped : Manifest](indexName:String, t: T, version: Long): Boolean = {
    val now = Calendar.getInstance().getTime
    t.lastUpdated = now
    val js = JacksonConverter.serialize(t)
    val req = com.sksamuel.elastic4s.ElasticDsl.update id t.uuid in indexName -> manifest[T].runtimeClass.getSimpleName version version doc new DocumentSource {
      override def json: String = js
    }
    val res = client.sync.execute(req)
    true
  }

  def searchAll[T: Manifest](req: SearchDefinition): Seq[T] = {
    val res = EsClient().execute(req)
    res.getHits.getHits.map { hit => JacksonConverter.deserialize[T](hit.getSourceAsString)}
  }

  def search[T: Manifest](req: SearchDefinition): Option[T] = {
    val res = EsClient().execute(req)
    if (res.getHits.getTotalHits == 0)
      None
    else
      Some(JacksonConverter.deserialize[T](res.getHits.getHits()(0).getSourceAsString))
  }

  def searchAllRaw(req: SearchDefinition): Array[SearchHit] = {
    val res = EsClient().execute(req)
    res.getHits.getHits
  }

  def searchRaw(req: SearchDefinition): Option[SearchHit] = {
    val res = EsClient().execute(req)
    if (res.getHits.getTotalHits == 0)
      None
    else
      Some(res.getHits.getHits()(0))
  }

  def esType[T: Manifest]: String = {
    val rt = manifest[T].runtimeClass
    rt.getSimpleName
  }
}
