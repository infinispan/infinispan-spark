package org.infinispan.spark.rdd

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.{Partition, TaskContext}
import org.infinispan.client.hotrod.Search
import org.infinispan.spark._

/**
  * Infinispan RDD filtered by a Query.
  *
  * @author gustavonalle
  */
class FilteredQueryInfinispanRDD[K, V, R](parent: InfinispanRDD[K, V], filter: QueryFilter) extends RDD[(K, R)](parent.sc, Nil) {

   @transient lazy val remoteCacheManager = {
      val remoteCacheManager = RemoteCacheManagerBuilder.create(parent.configuration)

      context.addSparkListener(new SparkListener {
         override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = remoteCacheManager.stop()
      })

      remoteCacheManager
   }

   override def count() = filter match {
      case f: StringQueryFilter =>
         val cache = getCache(parent.configuration, remoteCacheManager)
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

   override protected def getPartitions = parent.getPartitions

}