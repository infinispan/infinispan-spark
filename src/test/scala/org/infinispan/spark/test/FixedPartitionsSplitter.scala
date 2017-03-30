package org.infinispan.spark.test

import org.apache.spark.Partition
import org.infinispan.client.hotrod.CacheTopologyInfo
import org.infinispan.spark.config.ConnectorConfiguration
import org.infinispan.spark.rdd.{InfinispanPartition, Location, Splitter}

import scala.collection.JavaConversions._


/**
  * Creates a fixed number of partitions regardless of topology.
  */
class FixedPartitionsSplitter(numPartitions: Int) extends Splitter {

   @volatile var lastSplitCount = 0

   private def toJavaSet(s: Set[Int]) = new java.util.HashSet(s.map(e => e: java.lang.Integer))

   override def split(cacheTopology: CacheTopologyInfo, properties: ConnectorConfiguration): Array[Partition] = {
      val servers = cacheTopology.getSegmentsPerServer.keys.toList
      val serverIterator = Iterator.continually(servers.map(new Location(_))).flatten
      val segments = (0 until cacheTopology.getNumSegments).toSet
      val segmentSplits = cut[Int](segments, numPartitions)
      val result: Array[Partition] = segmentSplits.zipWithIndex.map {
         case (segs, idx) => new InfinispanPartition(new Integer(idx), serverIterator.next(), toJavaSet(segs), properties)
      }.toArray
      lastSplitCount = result.length
      result
   }

   private def cut[A](l: Set[A], parts: Int) = (0 until parts).map { i => l.drop(i).sliding(1, parts).flatten.toSet }.filter(_.nonEmpty)


}
