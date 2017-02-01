package org.infinispan.spark.rdd

import java.util.Properties

import org.apache.spark.api.java.{JavaPairRDD, JavaSparkContext}
import org.infinispan.query.dsl.Query
import org.infinispan.spark._

import scala.annotation.varargs
import scala.reflect.ClassTag

/**
 * @author gustavonalle
 */
object InfinispanJavaRDD {

   def createInfinispanRDD[K, V](jsc: JavaSparkContext, config: Properties): InfinispanJavaRDD[K, V] = {
      val infinispanRDD = new InfinispanRDD[K, V](jsc.sc, config, new PerServerSplitter)
      implicit val keyClassTag = ClassTag.AnyRef.asInstanceOf[ClassTag[K]]
      implicit val valueClassTag = ClassTag.AnyRef.asInstanceOf[ClassTag[V]]
      new InfinispanJavaRDD[K, V](infinispanRDD)
   }

   def write[K, V](pairRDD: JavaPairRDD[K, V], config: Properties) = pairRDD.rdd.writeToInfinispan(config)
}

class InfinispanJavaRDD[K, V](rdd: InfinispanRDD[K, V])
      (implicit override val kClassTag: ClassTag[K], implicit override val vClassTag: ClassTag[V])
      extends JavaPairRDD[K, V](rdd) {

   def filterByQuery[R](q: Query) = rdd.filterByQuery(q)

   def filterByQuery[R](q: String) = rdd.filterByQuery(q)

   @varargs def filterByCustom[R](filterFactory: String, params: AnyRef*): JavaPairRDD[K, R] = {
      val filteredRDD = rdd.filterByCustom[R](filterFactory, params:_*)
      implicit val converted = ClassTag.AnyRef.asInstanceOf[ClassTag[R]]
      JavaPairRDD.fromRDD[K,R](filteredRDD)
   }
}
