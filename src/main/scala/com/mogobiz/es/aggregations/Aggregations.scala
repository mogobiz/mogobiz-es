package com.mogobiz.es.aggregations

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.mappings.MappingDsl
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

object ElasticDsl2 extends IndexDsl
with AliasesDsl
with BulkDsl
with ClusterDsl
with CountDsl
with CreateIndexDsl
with DeleteIndexDsl
with DeleteDsl
with ExplainDsl
with AggregationDsl
with GetDsl
with MappingDsl
with MoreLikeThisDsl
with MultiGetDsl
with OptimizeDsl
with PercolateDsl
with PutMappingDsl
with SearchDsl
with NestedAggregationDsl
with ScoreDsl
with UpdateDsl
with ValidateDsl {

  import scala.concurrent.duration._

  implicit val duration: Duration = 10.seconds

}

trait NestedAggregationDsl{
  def nestedPath(path: String) = new NestedAggregationDefinition(path)
}

class NestedAggregationDefinition(path: String) extends AggregationDefinition[NestedAggregationDefinition, NestedBuilder] {
  val aggregationBuilder = AggregationBuilders.nested(path).path(path)
}
