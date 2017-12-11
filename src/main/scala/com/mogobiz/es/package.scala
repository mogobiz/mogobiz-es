/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz

import com.mogobiz.json.JacksonConverter
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.search.{SearchHit, SearchHits}
import org.elasticsearch.action.get.MultiGetItemResponse
import org.json4s
import org.json4s.jackson.JsonMethods._
import org.json4s.{JArray, JValue}

/**
  */
package object es {

  //implicits declaration
  implicit def searchHits2JValue(searchHits: SearchHits): JValue = {
    val jsvalues: Array[json4s.JValue] = searchHits.hits.map { hit =>
      parse(hit.sourceAsString)
    }
    JArray(jsvalues.toList)
  }

  implicit def hits2JArray(hits: Array[SearchHit]): JArray =
    JArray(hits.map(hit => parse(hit.sourceAsString)).toList)

  implicit def hit2JValue(hit: SearchHit): JValue = parse(hit.sourceAsString)

  implicit def response2JValue(response: GetResponse): JValue = parse(response.sourceAsString)

  implicit def responses2JArray(hits: Array[MultiGetItemResponse]): JArray =
    JArray(hits.map(hit => parse(hit.getResponse.getSourceAsString)).toList)

  implicit def JValue2StringSource(json: JValue): StringSource = new StringSource(JacksonConverter.asString(json))

  class StringSource(val str: String) {
    def json = str
  }

}
