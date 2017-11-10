package org.infinispan.spark.rdd

import org.apache.spark.api.java.{JavaPairRDD, JavaSparkContext}
import org.infinispan.query.dsl.Query
import org.infinispan.spark._
import org.infinispan.spark.config.ConnectorConfiguration

import scala.annotation.varargs
import scala.reflect.ClassTag

/**
  * @author gustavonalle
  */
object InfinispanJavaRDD {

   def createInfinispanRDD[K, V](jsc: JavaSparkContext, config: ConnectorConfiguration): InfinispanJavaRDD[K, V] = {
      createInfinispanRDD(jsc.sc, config, new PerServerSplitter)
   }

   def createInfinispanRDD[K, V](jsc: JavaSparkContext, config: ConnectorConfiguration, splitter: Splitter): InfinispanJavaRDD[K, V] = {
      val infinispanRDD = new InfinispanRDD[K, V](jsc.sc, config, splitter)
      implicit val keyClassTag = ClassTag.AnyRef.asInstanceOf[ClassTag[K]]
      implicit val valueClassTag = ClassTag.AnyRef.asInstanceOf[ClassTag[V]]
      new InfinispanJavaRDD[K, V](infinispanRDD)
   }

   def write[K, V](pairRDD: JavaPairRDD[K, V], config: ConnectorConfiguration) = pairRDD.rdd.writeToInfinispan(config)
}

class InfinispanJavaRDD[K, V](rdd: InfinispanRDD[K, V])
                             (implicit override val kClassTag: ClassTag[K], implicit override val vClassTag: ClassTag[V])
  extends JavaPairRDD[K, V](rdd) {

   def filterByQuery[R](q: Query): JavaPairRDD[K, R] = {
     val filteredRDD = rdd.filterByQuery[R](q)
     implicit val converted = ClassTag.AnyRef.asInstanceOf[ClassTag[R]]
     JavaPairRDD.fromRDD[K, R](filteredRDD)
   }

   def filterByQuery[R](q: String): JavaPairRDD[K, R] = {
     val filteredRDD = rdd.filterByQuery[R](q)
     implicit val converted = ClassTag.AnyRef.asInstanceOf[ClassTag[R]]
     JavaPairRDD.fromRDD[K, R](filteredRDD)
   }

   @varargs def filterByCustom[R](filterFactory: String, params: AnyRef*): JavaPairRDD[K, R] = {
      val filteredRDD = rdd.filterByCustom[R](filterFactory, params: _*)
      implicit val converted = ClassTag.AnyRef.asInstanceOf[ClassTag[R]]
      JavaPairRDD.fromRDD[K, R](filteredRDD)
   }
}
