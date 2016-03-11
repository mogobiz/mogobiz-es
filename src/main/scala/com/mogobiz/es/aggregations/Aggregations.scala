/*
 * Copyright (C) 2015 Mogobiz SARL. All rights reserved.
 */

package com.mogobiz.es.aggregations

import com.sksamuel.elastic4s._

object Aggregations {

  implicit class HistogramAggregationUtils(h: HistogramAggregation) {

    def minDocCount(minDocCount: Long): HistogramAggregation = {
      h.builder.minDocCount(minDocCount)
      h
    }
  }

}
