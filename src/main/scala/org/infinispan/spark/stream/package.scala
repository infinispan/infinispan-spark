package org.infinispan.spark

import java.util.Properties

import org.apache.spark.streaming.dstream.DStream

package object stream {

   implicit class InfinispanDStream[K, V](stream: DStream[(K, V)]) {
      def writeToInfinispan(configuration: Properties) = stream.foreachRDD(_.writeToInfinispan(configuration))
   }

}
