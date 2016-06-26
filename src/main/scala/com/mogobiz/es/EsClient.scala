/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.es

import java.util
import java.util.regex.Pattern
import java.util.{ Calendar, Date }

import com.mogobiz.json.JacksonConverter
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.ElasticDsl.{ index => esindex4s, update => esupdate4s, delete => esdelete4s, bulk => esbulk4s, _ }
import com.sksamuel.elastic4s.source.DocumentSource
import org.apache.commons.codec.binary.Base64
import org.elasticsearch.action.ActionRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.{ MultiGetItemResponse, GetResponse }
import org.elasticsearch.action.search.MultiSearchResponse
import org.elasticsearch.common.ContextAndHeaderHolder
import org.elasticsearch.common.collect.UnmodifiableIterator
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.index.get.GetResult
import org.elasticsearch.search.{ SearchHitField, SearchHits, SearchHit }
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

import Settings.ElasticSearch._
import Settings.ElasticSearch.Searchguard._

object EsClient {
  implicit val _ = Duration.Inf

  val settings = ImmutableSettings.settingsBuilder().put("cluster.name", Cluster).build()
  private val client: ElasticClient = ElasticClient.remote(settings, ElasticsearchClientUri(s"elasticsearch://$Host:$Port"))

  val credentials = Base64.encodeBase64String(s"$Username:$Password".getBytes)

  val MAX_SIZE = Integer.MAX_VALUE / 2

  def apply(): ElasticClient = {
    client
  }

  def secureActionRequest[T <: ActionRequestBuilder[_, _, _, _]](request: T): T = {
    if (Active)
      request.putHeader("searchguard_transport_creds", credentials)
    request
  }

  def secureRequest[T <: AnyRef { def build: ContextAndHeaderHolder }](request: T): T = {
    if (Active)
      request.build.putHeader("searchguard_transport_creds", credentials)
    request
  }

  type Timestamped = {
    val uuid: String
    var lastUpdated: Date
    var dateCreated: Date
  }

  def listIndices(pattern: String = null): Set[String] = {
    val clusterHealthResponse = secureActionRequest(client.admin.cluster().prepareHealth()).get()

    val matcher = Option(pattern).map(Pattern.compile)

    clusterHealthResponse.getIndices.keySet() flatMap {
      indice =>
        if (matcher.exists(_.matcher(indice).matches()))
          Some(indice)
        else
          None
    } toSet
  }

  def getIndexByAlias(alias: String): List[String] = {
    val aliases = secureActionRequest(client.admin.indices().prepareGetAliases(alias)).get().getAliases
    def extractListAlias(iterator: UnmodifiableIterator[String]): List[String] = {
      if (!iterator.hasNext) Nil
      else {
        iterator.next() :: extractListAlias(iterator)
      }
    }
    extractListAlias(aliases.keysIt())
  }

  def getUniqueIndexByAlias(alias: String): Option[String] = {
    val aliases = secureActionRequest(client.admin.indices().prepareGetAliases(alias)).get().getAliases
    val iterator = aliases.keysIt()
    if (iterator.hasNext) Some(iterator.next())
    else None
  }

  def indexLowercase[T: Manifest](store: String, t: T, refresh: Boolean = false): String = {
    val js = JacksonConverter.serialize(t)
    val req = esindex4s into (store, manifest[T].runtimeClass.getSimpleName.toLowerCase) doc new DocumentSource {
      override val json: String = js
    } refresh refresh
    val res = client.execute(secureRequest(req)).await
    res.getId
  }

  def index[T <: Timestamped: Manifest](indexName: String, t: T, refresh: Boolean, id: Option[String] = None): String = {
    val now = Calendar.getInstance().getTime
    t.dateCreated = now
    t.lastUpdated = now
    val json = JacksonConverter.serialize(t)
    val res = secureActionRequest(client.client.prepareIndex(indexName, manifest[T].runtimeClass.getSimpleName, id.getOrElse(t.uuid)))
      .setSource(json)
      .setRefresh(refresh)
      .execute()
      .actionGet()
    res.getId
  }

  def simpleIndex[T: Manifest](indexName: String, t: T, refresh: Boolean, id: String): String = {
    val json = JacksonConverter.serialize(t)
    val res = client.client.prepareIndex(indexName, manifest[T].runtimeClass.getSimpleName, id)
      .setSource(json)
      .setRefresh(refresh)
      .execute()
      .actionGet()
    res.getId
  }

  def exists(indexes: String*): Boolean = {
    client.execute {
      indexExists(indexes) //FIXME
    }.await.isExists
  }

  def load[T: Manifest](indexName: String, uuid: String): Option[T] = {
    load[T](indexName, uuid, manifest[T].runtimeClass.getSimpleName)
  }

  def load[T: Manifest](indexName: String, uuid: String, esDocumentName: String): Option[T] = {
    val req = get id uuid from indexName -> esDocumentName
    val res = client.execute(secureRequest(req)).await
    if (res.isExists) Some(JacksonConverter.deserialize[T](res.getSourceAsString)) else None
  }

  def loadWithVersion[T: Manifest](indexName: String, uuid: String): Option[(T, Long)] = {
    val req = get id uuid from indexName -> manifest[T].runtimeClass.getSimpleName
    val res = client.execute(secureRequest(req)).await
    val maybeT = if (res.isExists) Some(JacksonConverter.deserialize[T](res.getSourceAsString)) else None
    maybeT map ((_, res.getVersion))
  }

  def loadRaw(req: GetDefinition): Option[GetResponse] = {
    val res = client.execute(secureRequest(req)).await
    if (res.isExists) Some(res) else None
  }

  def loadRaw(req: MultiGetDefinition): Array[MultiGetItemResponse] = {
    client.execute(req).await.getResponses
  }

  def delete[T: Manifest](indexName: String, uuid: String, refresh: Boolean): Boolean = {
    val req = esdelete4s id uuid from indexName -> manifest[T].runtimeClass.getSimpleName refresh refresh
    val res = client.execute(secureRequest(req)).await
    res.isFound
  }

  def updateRaw(req: UpdateDefinition): GetResult = {
    client.execute(secureRequest(req)).await.getGetResult
  }

  def deleteRaw(req: DeleteByIdDefinition): Unit = {
    client.execute(secureRequest(req))
  }

  def bulk(requests: Seq[BulkCompatibleDefinition]): BulkResponse = {
    val req = esbulk4s(requests: _*)
    val res = client.execute(secureRequest(req)).await
    res
  }

  def update[T <: Timestamped: Manifest](indexName: String, t: T, upsert: Boolean, refresh: Boolean): Boolean = {
    update[T](indexName, t, manifest[T].runtimeClass.getSimpleName, upsert, refresh)
  }

  def update[T <: Timestamped: Manifest](indexName: String, t: T, esDocumentName: String, upsert: Boolean, refresh: Boolean): Boolean = {
    val now = Calendar.getInstance().getTime
    t.lastUpdated = now
    val js = JacksonConverter.serialize(t)
    val req = esupdate4s id t.uuid in indexName -> esDocumentName refresh refresh doc new DocumentSource {
      override def json: String = js
    }
    req.docAsUpsert(upsert)
    val res = client.execute(secureRequest(req)).await
    res.isCreated || res.getVersion > 1
  }

  def update[T <: Timestamped: Manifest](indexName: String, t: T, version: Long): Boolean = {
    val now = Calendar.getInstance().getTime
    t.lastUpdated = now
    val js = JacksonConverter.serialize(t)
    val req = esupdate4s id t.uuid in indexName -> manifest[T].runtimeClass.getSimpleName version version doc new DocumentSource {
      override def json: String = js
    }
    client.execute(secureRequest(req)).await
    true
  }

  def searchAll[T: Manifest](req: SearchDefinition, fieldsDeserialize: (T, util.Map[String, SearchHitField]) => T = { (hit: T, fields: util.Map[String, SearchHitField]) => hit }): Seq[T] = {
    debug(req)
    val res = client.execute(secureRequest(req)).await
    res.getHits.getHits.map { hit =>
      {
        fieldsDeserialize(JacksonConverter.deserialize[T](hit.getSourceAsString), hit.fields())
      }
    }
  }

  def search[T: Manifest](req: SearchDefinition): Option[T] = {
    debug(req)
    val res = client.execute(secureRequest(req)).await
    if (res.getHits.getTotalHits == 0)
      None
    else
      Some(JacksonConverter.deserialize[T](res.getHits.getHits()(0).getSourceAsString))
  }

  def searchAllRaw(req: SearchDefinition): SearchHits = {
    debug(req)
    val res = client.execute(secureRequest(req)).await
    res.getHits
  }

  def searchRaw(req: SearchDefinition): Option[SearchHit] = {
    debug(req)
    val res = client.execute(secureRequest(req)).await
    if (res.getHits.getTotalHits == 0)
      None
    else
      Some(res.getHits.getHits()(0))
  }

  def esType[T: Manifest]: String = {
    val rt = manifest[T].runtimeClass
    rt.getSimpleName
  }

  def multiSearchRaw(req: List[SearchDefinition]): Array[Option[SearchHits]] = {
    req.foreach(debug)
    val multiSearchResponse: MultiSearchResponse = client.execute(secureRequest(multi(req: _*))).await
    for (resp <- multiSearchResponse.getResponses) yield {
      if (resp.isFailure)
        None
      else
        Some(resp.getResponse.getHits)
    }
  }

  def multiSearchAgg(req: List[SearchDefinition]): JValue = {
    req.foreach(debug)
    val multiSearchResponse: MultiSearchResponse = client.execute(secureRequest(multi(req: _*))).await
    val esResult = for (resp <- multiSearchResponse.getResponses) yield {
      if (resp.isFailure) None
      else {
        val resJson = parse(resp.getResponse.toString)
        Some(resJson \ "aggregations")
      }
    }
    join(esResult.toList.flatten)
  }

  private def join(list: List[JValue]): JValue = {
    if (list.isEmpty) JNothing
    else list.head merge join(list.tail)
  }

  /**
   * send back the aggregations results
   *
   * @param req - request
   * @return
   */
  def searchAgg(req: SearchDefinition): JValue = {
    debug(req)
    val res = client.execute(secureRequest(req)).await
    val resJson = parse(res.toString)
    resJson \ "aggregations"
  }

  import Settings._
  import com.typesafe.scalalogging.Logger

  private val logger = Logger(LoggerFactory.getLogger("esClient"))

  private def debug(req: SearchDefinition) {
    if (ElasticSearch.EsDebug) {
      logger.info(req._builder.toString)
    }
  }

  import akka.stream.scaladsl._

  def bulkBalancedFlow(bulkSize: Int = Settings.ElasticSearch.bulkSize, balanceSize: Int = 2) =
    Flow() { implicit b =>
      import FlowGraphImplicits._

      val in = UndefinedSource[BulkCompatibleDefinition]
      val group = Flow[BulkCompatibleDefinition].grouped(bulkSize)
      val bulkUpsert = Flow[Seq[BulkCompatibleDefinition]].map(bulk)
      val out = UndefinedSink[BulkResponse]

      if (balanceSize > 1) {

        val balance = Balance[Seq[BulkCompatibleDefinition]]
        val merge = Merge[BulkResponse]

        in ~> group ~> balance
        1 to balanceSize foreach { _ =>
          balance ~> bulkUpsert ~> merge
        }
        merge ~> out

      } else {
        in ~> group ~> bulkUpsert ~> out
      }

      (in, out)
    }
}
