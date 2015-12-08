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
      if (args.length < 1) {
         System.out.println("Usage: WordCountScala <infinispan_host>")
         System.exit(1)
      }

      Logger.getLogger("org").setLevel(Level.WARN)
      val infinispanHost = args(0)

      val conf = new SparkConf().setAppName("spark-infinispan-wordcount-scala")
      val sc = new SparkContext(conf)

      val stopWords =
         fromInputStream(classOf[WordCountJava].getClassLoader.getResourceAsStream("stopWords.txt")).getLines().toSet

      val infinispanProperties = new Properties
      infinispanProperties.put("infinispan.client.hotrod.server_list", infinispanHost)

      val infinispanRDD = new InfinispanRDD[Long, Tweet](sc, configuration = infinispanProperties)

      val results = infinispanRDD.values
              .flatMap(_.getText.split(" "))
              .map(_.replaceAll("[^a-zA-Z ]", ""))
              .filter(s => !stopWords.contains(s.toLowerCase) && s.nonEmpty)
              .map((_, 1))
              .reduceByKey(_ + _)
              .sortBy(_._2, ascending = false)
              .take(20)

      results.foreach { case (word, count) => println(s"'$word' appears $count times") }
   }

}

