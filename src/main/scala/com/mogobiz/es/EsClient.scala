package com.mogobiz.es

import java.util.{Calendar, Date}

import com.mogobiz.json.JacksonConverter
import com.sksamuel.elastic4s.{BulkCompatibleDefinition, MultiGetDefinition, GetDefinition, ElasticClient}
import com.sksamuel.elastic4s.ElasticDsl.{index => esindex4s, update => esupdate4s, delete => esdelete4s, bulk => esbulk4s, _}
import com.sksamuel.elastic4s.source.DocumentSource
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.{MultiGetItemResponse, GetResponse}
import org.elasticsearch.action.search.MultiSearchResponse
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.index.get.GetResult
import org.elasticsearch.search.{SearchHits, SearchHit}
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory

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


  def index[T: Manifest](store: String, t: T): String = {
    val js = JacksonConverter.serialize(t)
    val req = esindex4s into(store, manifest[T].runtimeClass.getSimpleName.toLowerCase) doc new DocumentSource {
      override val json: String = js
    }
    val res = EsClient().execute(req)
    res.getId
  }

  def index[T <: Timestamped : Manifest](indexName:String, t: T, refresh: Boolean = false): String = {
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

  def loadRaw(req:GetDefinition): Option[GetResponse] = {
    val res = EsClient().execute(req)
    if (res.isExists) Some(res) else None
  }

  def loadRaw(req:MultiGetDefinition): Array[MultiGetItemResponse] = {
    EsClient().execute(req).getResponses
  }

  def delete[T: Manifest](indexName:String, uuid: String, refresh: Boolean): Boolean = {
    val req = esdelete4s id uuid from indexName -> manifest[T].runtimeClass.getSimpleName refresh refresh
    val res = client.sync.execute(req)
    res.isFound
  }

  def updateRaw(req:UpdateDefinition) : GetResult = {
    EsClient().execute(req).getGetResult
  }

  def bulk(requests: Seq[BulkCompatibleDefinition]) : BulkResponse = {
    val req = esbulk4s(requests:_*)
    val res = EsClient().execute(req)
    res
  }

  def update[T <: Timestamped : Manifest](indexName:String, t: T, upsert: Boolean, refresh: Boolean): Boolean = {
    update[T](indexName, t, manifest[T].runtimeClass.getSimpleName, upsert, refresh)
  }

  def update[T <: Timestamped : Manifest](indexName:String, t: T, esDocumentName:String, upsert: Boolean, refresh: Boolean): Boolean = {
    val now = Calendar.getInstance().getTime
    t.lastUpdated = now
    val js = JacksonConverter.serialize(t)
    val req = esupdate4s id t.uuid in indexName -> esDocumentName refresh refresh doc new DocumentSource {
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
    val req = esupdate4s id t.uuid in indexName -> manifest[T].runtimeClass.getSimpleName version version doc new DocumentSource {
      override def json: String = js
    }
    client.sync.execute(req)
    true
  }

  def searchAll[T: Manifest](req: SearchDefinition): Seq[T] = {
    debug(req)
    val res = EsClient().execute(req)
    res.getHits.getHits.map { hit => JacksonConverter.deserialize[T](hit.getSourceAsString)}
  }

  def search[T: Manifest](req: SearchDefinition): Option[T] = {
    debug(req)
    val res = EsClient().execute(req)
    if (res.getHits.getTotalHits == 0)
      None
    else
      Some(JacksonConverter.deserialize[T](res.getHits.getHits()(0).getSourceAsString))
  }

  def searchAllRaw(req: SearchDefinition): SearchHits = {
    debug(req)
    val res = EsClient().execute(req)
    res.getHits
  }

  def searchRaw(req: SearchDefinition): Option[SearchHit] = {
    debug(req)
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

  def multiSearchRaw(req: List[SearchDefinition]): Array[Option[SearchHits]] = {
    req.foreach(debug)
    val multiSearchResponse:MultiSearchResponse = EsClient().execute(req:_*)
    for(resp <- multiSearchResponse.getResponses) yield {
      if(resp.isFailure)
        None
      else
        Some(resp.getResponse.getHits)
    }
  }

  def multiSearchAgg(req: List[SearchDefinition]): JValue = {
    req.foreach(debug)
    val multiSearchResponse:MultiSearchResponse = EsClient().execute(req:_*)
    val esResult = for(resp <- multiSearchResponse.getResponses) yield {
      if(resp.isFailure) None
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
   * @param req - request
   * @return
   */
  def searchAgg(req: SearchDefinition) : JValue = {
    debug(req)
    val res = EsClient().execute(req)
    val resJson = parse(res.toString)
    resJson \ "aggregations"
  }

  import Settings._
  import com.typesafe.scalalogging.slf4j.Logger

  private val logger = Logger(LoggerFactory.getLogger("esClient"))

  private def debug(req: SearchDefinition) {
    if (ElasticSearch.EsDebug) {
      logger.info(req._builder.toString)
    }
  }

  import akka.stream.scaladsl._

  def bulkBalancedFlow(bulkSize: Int = 100, balanceSize: Int = 2) = Flow(){implicit b =>
    import FlowGraphImplicits._

    val in = UndefinedSource[BulkCompatibleDefinition]
    val grouped = Flow[BulkCompatibleDefinition].grouped(bulkSize)
    val bulkFlow = Flow[Seq[BulkCompatibleDefinition]].map(bulk)
    val out = UndefinedSink[BulkResponse]

    if(balanceSize > 1){

      val balance = Balance[Seq[BulkCompatibleDefinition]]
      val merge = Merge[BulkResponse]

      in ~> grouped ~> balance
      1 to balanceSize foreach { _ =>
        balance ~> bulkFlow ~> merge
      }
      merge ~> out

    }
    else{
      in ~> grouped ~> bulkFlow ~> out
    }

    (in, out)
  }

}
