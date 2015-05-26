package com.mogobiz

import com.sksamuel.elastic4s.source.DocumentSource
import org.elasticsearch.action.get.{MultiGetItemResponse, GetResponse}
import org.elasticsearch.common.xcontent.{ToXContent, XContentFactory}
import org.elasticsearch.search.{SearchHit, SearchHits}
import org.json4s.JsonAST.{JArray, JValue}
import org.json4s.native.JsonMethods._

/**
 * Created by yoannbaudy on 05/03/2015.
 */
package object es {

  //implicits declaration
  implicit def searchHits2JValue(searchHits:SearchHits) : JValue = {
    parse(searchHits.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string().substring("hits".length + 2))
  }

  implicit def hits2JArray(hits:Array[SearchHit]) : JArray = JArray(hits.map(hit => parse(hit.getSourceAsString)).toList)

  implicit def hit2JValue(hit:SearchHit) : JValue = parse(hit.getSourceAsString)

  implicit def response2JValue(response:GetResponse) : JValue = parse(response.getSourceAsString)

  implicit def responses2JArray(hits:Array[MultiGetItemResponse]) : JArray = JArray(hits.map(hit => parse(hit.getResponse.getSourceAsString)).toList)

  implicit def JValue2StringSource(json:JValue) : StringSource = new StringSource(compact(render(json)))

  class StringSource(val str:String) extends DocumentSource {
    def json = str
  }

}