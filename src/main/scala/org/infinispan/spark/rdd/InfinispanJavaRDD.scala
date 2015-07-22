package org.infinispan.spark.rdd

import java.util.Properties

import org.apache.spark.api.java.{JavaPairRDD, JavaSparkContext}
import org.infinispan.spark._

import scala.reflect.ClassTag

/**
 * @author gustavonalle
 */
object InfinispanJavaRDD {

   def createInfinispanRDD[K, V](jsc: JavaSparkContext, config: Properties) = {
      val infinispanRDD = new InfinispanRDD[K, V](jsc.sc, config, new PerServerSplitter)
      implicit val keyClassTag = ClassTag.AnyRef.asInstanceOf[ClassTag[K]]
      implicit val valueClassTag = ClassTag.AnyRef.asInstanceOf[ClassTag[V]]
      JavaPairRDD.fromRDD(infinispanRDD)
   }

   def write[K, V](pairRDD: JavaPairRDD[K, V], config: Properties) = pairRDD.rdd.writeToInfinispan(config)


}
