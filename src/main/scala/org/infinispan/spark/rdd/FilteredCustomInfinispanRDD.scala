package org.infinispan.spark.rdd

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

/**
  * Infinispan RDD filtered by a deployed filter.
  *
  * @author gustavonalle
  */
class FilteredCustomInfinispanRDD[K, V, R](parent: InfinispanRDD[K, V], filter: DeployedFilter) extends RDD[(K, R)](parent.sc, Nil) {

   private def getIteratorSize[T](iterator: Iterator[T]) = iterator.size

   override def count() = parent.sc.runJob(this, getIteratorSize _).sum

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

