/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.es

import com.sksamuel.elastic4s.http.search.SearchHit
import org.json4s.JsonAST.{JArray, JValue}
import org.json4s.jackson.JsonMethods._

/**
  */
object Es4Json {

  implicit def searchHits2JArray(searchHits: Array[SearchHit]): JArray = {
    JArray(searchHits.map(hit => parse(hit.sourceAsString)).toList)
  }

  implicit def searchHit2JValue(searchHit: SearchHit): JValue = {
    parse(searchHit.sourceAsString)
  }

}
