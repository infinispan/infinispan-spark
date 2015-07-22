package org.infinispan.spark.rdd

import java.util.Map.Entry

import org.apache.spark.TaskContext
import org.infinispan.commons.util.CloseableIterator

/**
 * @author gustavonalle
 */
class InfinispanIterator[K, V](val closeableIterator: CloseableIterator[Entry[AnyRef, AnyRef]], context: TaskContext) extends Iterator[(K, V)] {

   override def hasNext: Boolean = closeableIterator.hasNext

   override def next(): (K, V) = {
      val entry = closeableIterator.next
      (entry.getKey.asInstanceOf[K], entry.getValue.asInstanceOf[V])
   }
}

