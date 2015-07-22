package org.infinispan.spark.examples.twitter

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.infinispan.spark.rdd.InfinispanRDD

import scala.io.Source.fromInputStream

/**
 * @author gustavonalle
 */
object WordCountScala {

   def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.WARN)

      val conf = new SparkConf().setAppName("spark-infinispan-wordcount-scala")
      val sc = new SparkContext(conf)
      val master = sc.getConf.get("spark.master").replace("spark://", "").replaceAll(":.*", "")

      val stopWords =
         fromInputStream(classOf[WordCountJava].getClassLoader.getResourceAsStream("stopWords.txt")).getLines().toSet

      val infinispanProperties = new Properties
      infinispanProperties.put("infinispan.client.hotrod.server_list", master)

      val infinispanRDD = new InfinispanRDD[Long, Tweet](sc, configuration = infinispanProperties)

      val results = infinispanRDD.values
              .flatMap(_.getText.split(" "))
              .map(_.replaceAll("[^a-zA-Z ]", ""))
              .filter(s => !stopWords.contains(s.toLowerCase))
              .map((_, 1))
              .reduceByKey(_ + _)
              .sortBy(_._2, ascending = false)
              .take(20)

      results.foreach { case (word, count) => println(s"'$word' appears $count times") }
   }

}

