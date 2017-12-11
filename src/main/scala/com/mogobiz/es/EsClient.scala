/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.es

import java.util.regex.Pattern
import java.util.{Calendar, Date}

import cats.Show
import com.mogobiz.es.Settings.ElasticSearch._
import com.mogobiz.json.JacksonConverter
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.bulk.BulkCompatibleDefinition
import com.sksamuel.elastic4s.delete.DeleteByIdDefinition
import com.sksamuel.elastic4s.get.{GetDefinition, MultiGetDefinition}
import com.sksamuel.elastic4s.http.ElasticDsl.{bulk => esbulk4s, delete => esdelete4s, update => esupdate4s, _}
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchHits, SearchImplicits, SearchResponse}
import com.sksamuel.elastic4s.http.update.UpdateImplicits.UpdateHttpExecutable
import com.sksamuel.elastic4s.http.{HttpClient, HttpExecutable}
import com.sksamuel.elastic4s.searches.SearchDefinition
import com.sksamuel.elastic4s.update.UpdateDefinition
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object EsClient extends SearchImplicits with LazyLogging {
  implicit val _         = Duration.Inf
  val client: HttpClient = HttpClient(ElasticsearchClientUri(FullUrl))

  def apply(): HttpClient = {
    client
  }

  val MaxSize: Int = Int.MaxValue / 2

  type Timestamped = {
    val uuid: String
    var lastUpdated: Date
    var dateCreated: Date
  }

  def listIndices(pattern: String = null): Set[String] = {
    val matcher = Option(pattern).map(Pattern.compile)
    client
      .execute(catIndices())
      .await
      .filter(index => matcher.exists(_.matcher(index.index).matches()))
      .map(_.index)
      .toSet
  }

  def getIndexByAlias(alias: String): List[String] = client.execute(getAlias(alias)).await.keys.toList

  def getUniqueIndexByAlias(alias: String): Option[String] = getIndexByAlias(alias).headOption

  def indexLowercase[T <: AnyRef: Manifest](store: String, t: T, refresh: Boolean = false): String = {
    val js        = JacksonConverter.serialize(t)
    val doRefresh = if (refresh) RefreshPolicy.IMMEDIATE else RefreshPolicy.NONE
    client
      .execute(indexInto(store, manifest[T].runtimeClass.getSimpleName.toLowerCase) doc js refresh doRefresh)
      .await
      .id
  }

  def index[T <: Timestamped: Manifest](indexName: String, t: T, refresh: Boolean, id: Option[String] = None): String = {
    val now = Calendar.getInstance().getTime
    t.dateCreated = now
    t.lastUpdated = now
    val json      = JacksonConverter.serialize(t)
    val doRefresh = if (refresh) RefreshPolicy.IMMEDIATE else RefreshPolicy.NONE
    client
      .execute(
          indexInto(indexName, manifest[T].runtimeClass.getSimpleName)
            .withId(id.getOrElse(t.uuid)) doc json refresh doRefresh
      )
      .await
      .id
  }

  def simpleIndex[T <: AnyRef: Manifest](indexName: String, t: T, refresh: Boolean, id: String): String = {
    val json      = JacksonConverter.serialize(t)
    val doRefresh = if (refresh) RefreshPolicy.IMMEDIATE else RefreshPolicy.NONE
    client
      .execute(
          indexInto(indexName, manifest[T].runtimeClass.getSimpleName).withId(id) doc json refresh doRefresh
      )
      .await
      .id
  }

  def exists(indexes: String*): Boolean = {
    Future
      .sequence(indexes.map { index =>
        client.execute(indexExists(index))
      })
      .await
      .forall(response => response.exists)
  }

  def load[T: Manifest](indexName: String, uuid: String): Option[T] = {
    load[T](indexName, uuid, manifest[T].runtimeClass.getSimpleName)
  }

  def load[T: Manifest](indexName: String, uuid: String, esTypeName: String): Option[T] = {
    val res = client.execute(get(uuid) from indexName -> esTypeName).await
    if (res.found) Some(JacksonConverter.deserialize[T](res.sourceAsString)) else None
  }

  def loadWithVersion[T: Manifest](indexName: String, uuid: String): Option[(T, Long)] = {
    val res    = client.execute(get(uuid) from indexName -> manifest[T].runtimeClass.getSimpleName).await
    val maybeT = if (res.found) Some(JacksonConverter.deserialize[T](res.sourceAsString)) else None
    maybeT map ((_, res.version))
  }

  def loadRaw(req: GetDefinition): Option[GetResponse] = {
    val res: GetResponse = client.execute(req).await
    if (res.found) Some(res) else None
  }

  def loadRaw(req: MultiGetDefinition): Seq[GetResponse] = {
    client.execute(req).await.items
  }

  def delete[T: Manifest](indexName: String, uuid: String, refresh: Boolean): Boolean = {
    delete(indexName, uuid, manifest[T].runtimeClass.getSimpleName, refresh)
  }

  def delete[T: Manifest](indexName: String, uuid: String, esDocumentName: String, refresh: Boolean): Boolean = {
    val doRefresh = if (refresh) RefreshPolicy.IMMEDIATE else RefreshPolicy.NONE
    val req       = esdelete4s(uuid) from indexName -> esDocumentName refresh doRefresh
    client.execute(esdelete4s(uuid) from indexName -> esDocumentName refresh doRefresh).await.found
  }

  def bulk(requests: Seq[BulkCompatibleDefinition]): BulkResponse = client.execute(esbulk4s(requests: _*)).await

  def update[T <: Timestamped: Manifest](indexName: String, t: T, upsert: Boolean, refresh: Boolean): Boolean =
    update[T](indexName, t, manifest[T].runtimeClass.getSimpleName, upsert, refresh)

  def update[T <: Timestamped: Manifest](indexName: String,
                                         t: T,
                                         esDocumentName: String,
                                         upsert: Boolean,
                                         refresh: Boolean): Boolean = {
    val now = Calendar.getInstance().getTime
    t.lastUpdated = now
    val js        = JacksonConverter.serialize(t)
    val doRefresh = if (refresh) RefreshPolicy.IMMEDIATE else RefreshPolicy.NONE
    val req       = esupdate4s(t.uuid) in indexName -> esDocumentName refresh doRefresh doc js docAsUpsert (upsert)
    client.execute(req).await.result
    true
  }

  def update[T <: Timestamped: Manifest](indexName: String, t: T, version: Long): Boolean = {
    val now = Calendar.getInstance().getTime
    t.lastUpdated = now
    val js                    = JacksonConverter.serialize(t)
    val req: UpdateDefinition = esupdate4s(t.uuid) in indexName -> manifest[T].runtimeClass.getSimpleName version version doc js
    client.execute(req).await
    true
  }

  def updateRaw(req: UpdateDefinition): String = {
    submit(req).await.result
  }

  def deleteRaw(req: DeleteByIdDefinition): String = {
    apply().execute(req).await.result
  }

  def searchAll[T: Manifest](req: SearchDefinition, fieldsDeserialize: (T, Map[String, Any]) => T = {
    (hit: T, fields: Map[String, Any]) =>
      hit
  }): Seq[T] = {
    debug(req)
    val res: SearchResponse = client.execute(req).await
    res.hits.hits.map { (hit: SearchHit) =>
      {
        fieldsDeserialize(JacksonConverter.deserialize[T](hit.sourceAsString), hit.fields)
      }
    }
  }

  def search[T: Manifest](req: SearchDefinition): Option[T] = {
    debug(req)
    val res = client.execute(req).await
    if (res.hits.total == 0)
      None
    else
      Some(JacksonConverter.deserialize[T](res.hits.hits(0).sourceAsString))
  }

  def searchAllRaw(req: SearchDefinition): SearchHits = {
    submit(req).await.hits
  }

  def searchRaw(req: SearchDefinition): Option[JValue] = {
    val res = submit(req).await
    if (res.totalHits == 0)
      None
    else {
      Some(parse(res.hits.hits.head.sourceAsString))
    }

  }

  def esType[T: Manifest]: String = {
    val rt = manifest[T].runtimeClass
    rt.getSimpleName
  }

  def multiSearchRaw(reqs: List[SearchDefinition]): Seq[Option[SearchHits]] = {
    reqs.foreach(req => debug(req))
    val multiSearchResponse = client.execute(multi(reqs)).await
    for (resp <- multiSearchResponse.responses) yield {
      if (resp.isEmpty)
        None
      else
        Some(resp.hits)
    }
  }

  def multiSearchAgg(reqs: List[SearchDefinition]): JValue = {
    reqs.foreach(req => debug(req))
    val multiSearchResponse = client.execute(multi(reqs)).await.responses
    val esResult = for (resp <- multiSearchResponse) yield {
      parse(resp.aggregationsAsString)
    }
    join(esResult)
  }

  private def join(list: Seq[JValue]): JValue = {
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
    val res     = submit(req).await
    val resJson = parse(res.toString)
    resJson \ "aggregations"
  }

  def makeIndex(index: String, typ: String): String = index + "_" + typ

  def delIndex(index: String, typ: String) = {
    client.execute(deleteIndex(makeIndex(index, typ)))
  }

  def delIndex(indexAndTyp: (String, String)) = {
    client.execute(deleteIndex(makeIndex(indexAndTyp._1, indexAndTyp._2)))
  }

  import Settings._

  def debug[T](req: T)(implicit ev: Show[T]) = {
    if (ElasticSearch.EsDebug) {
      logger.info(client.show(req))
    }
  }

  def submit[T, U](req: T)(implicit ev1: HttpExecutable[T, U], ev2: Show[T]): Future[U] = {
    debug(req)
    apply().execute(req)
  }

  import akka.stream.scaladsl._

  def bulkBalancedFlow(bulkSize: Int = Settings.ElasticSearch.bulkSize,
                       balanceSize: Int = 2): Flow[BulkCompatibleDefinition, BulkResponse] =
    Flow() { implicit b =>
      import FlowGraphImplicits._

      val in         = UndefinedSource[BulkCompatibleDefinition]
      val group      = Flow[BulkCompatibleDefinition].grouped(bulkSize)
      val bulkUpsert = Flow[Seq[BulkCompatibleDefinition]].map(bulk)
      val out        = UndefinedSink[BulkResponse]

      if (balanceSize > 1) {

        val balance = Balance[Seq[BulkCompatibleDefinition]]
        val merge   = Merge[BulkResponse]

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
