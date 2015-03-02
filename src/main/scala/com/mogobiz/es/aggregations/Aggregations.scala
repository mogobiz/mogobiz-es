package com.mogobiz.es.aggregations

import com.sksamuel.elastic4s._
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder


object Aggregations {

  implicit class HistogramAggregationUtils(h: HistogramAggregation){

    def minDocCount(minDocCount: Long): HistogramAggregation = {
      h.builder.minDocCount(minDocCount)
      h
    }
  }

}
