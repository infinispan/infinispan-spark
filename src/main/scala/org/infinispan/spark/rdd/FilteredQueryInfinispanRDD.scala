package org.infinispan.spark.rdd

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}
import org.infinispan.client.hotrod.Search
import org.infinispan.spark._

/**
  * Infinispan RDD filtered by a Query.
  *
  * @author gustavonalle
  */
class FilteredQueryInfinispanRDD[K, V, R](parent: InfinispanRDD[K, V], filter: QueryFilter) extends RDD[(K, R)](parent.sc, Nil) {

   override def count(): Long = filter match {
      case f: StringQueryFilter =>
         val cache = getCache(parent.configuration, parent.remoteCacheManager)
         Search.getQueryFactory(cache).create(f.queryString).getResultSize
      case f: QueryObjectFilter => f.query.getResultSize
   }


   @DeveloperApi
   override def compute(split: Partition, context: TaskContext): Iterator[(K, R)] = parent.compute(
      split,
      context,
      (a, p) => RemoteCacheManagerBuilder.create(p, a),
      filter.name,
      filter.params.toArray
   )

   override protected def getPartitions: Array[Partition] = parent.getPartitions

}