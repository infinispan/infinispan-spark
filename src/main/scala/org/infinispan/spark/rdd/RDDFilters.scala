package org.infinispan.spark.rdd

import org.infinispan.client.hotrod.filter.Filters
import org.infinispan.query.dsl.Query
import org.infinispan.query.dsl.impl.BaseQuery

trait Filter {
   def name: String

   def params: Seq[AnyRef]

}

private case class DeployedFilter(name: String, params: AnyRef*) extends Filter

trait QueryFilter extends Filter {
   def name = Filters.ITERATION_QUERY_FILTER_CONVERTER_FACTORY_NAME

   def queryString: String

   def params = Seq(queryString)
}


private case class QueryObjectFilter(@transient query: Query) extends QueryFilter {
   val queryString = query.asInstanceOf[BaseQuery].getQueryString
}

private case class StringQueryFilter(queryString: String) extends QueryFilter


