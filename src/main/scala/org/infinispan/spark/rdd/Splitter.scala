package org.infinispan.spark.rdd

import java.net.SocketAddress
import java.util
import java.util.Properties

import org.apache.spark.Partition
import org.infinispan.client.hotrod.CacheTopologyInfo
import org.infinispan.spark._
import scala.collection.JavaConversions._

import scala.collection.mutable


/**
  * The Splitter is responsible to produce one or more [[org.infinispan.spark.rdd.InfinispanPartition]] that will
  * execute the job on a certain slice of the data. On Infinispan, data is spread roughly equally across multiple
  * segments.
  *
  * @author gustavonalle
  */
trait Splitter {

   /**
     * Creates partitions based on a certain cluster topology.
     *
     * @param cacheTopology [[CacheTopologyInfo]] holding information of servers and segments ownership.
     * @param properties [[Properties]] used to configure the RDD
     * @return an array of [[InfinispanPartition]]
     */
   def split(cacheTopology: CacheTopologyInfo, properties: Properties): Array[Partition]
}

/**
  * Create one or more partition per server so that:
  * - Each partition will only contain segments owned by the associated server
  * - A segment is unique across partitions
  */
class PerServerSplitter extends Splitter {

   override def split(cacheTopology: CacheTopologyInfo, properties: Properties) = {
      val segmentsByServer = cacheTopology.getSegmentsPerServer
      if (segmentsByServer.isEmpty) throw new IllegalArgumentException("No servers found to partition")
      if (segmentsByServer.keySet().size == 1 && segmentsByServer.values.flatten.isEmpty) {
         Array(new SingleServerPartition(segmentsByServer.keySet.head, properties))
      } else {
         val segmentsByServerSeq = segmentsByServer.toStream.sortBy { case (_, v) => v.size }
         val segments = segmentsByServerSeq.flatMap { case (address, segs) => segs.toSeq }.distinct

         val numServers = segmentsByServerSeq.size
         val numSegments = segments.size
         val segmentsPerServer = Math.ceil(numSegments.toFloat / numServers.toFloat).toInt

         val q = mutable.Queue(segments: _*)
         val segmentsByServerIterator = Iterator.continually(segmentsByServerSeq).flatten
         val result = new mutable.HashMap[SocketAddress, collection.mutable.Set[Integer]] with mutable.MultiMap[SocketAddress, Integer]
         while (q.nonEmpty) {
            val (server, segments) = segmentsByServerIterator.next()
            val split = List.fill(segmentsPerServer) {
               q.dequeueFirst(segments.contains)
            }.flatten
            if (split.nonEmpty) {
               split.foreach {
                  result.addBinding(server, _)
               }
            }
         }

         val pps = properties.readWithDefault[Int](InfinispanRDD.PartitionsPerServer)(default = InfinispanRDD.DefaultPartitionsPerServer)
         result.toStream.flatMap { case (a, b) => cut(b.toSeq, pps).map((a, _)) }.zipWithIndex.map { case ((server, segs), idx) =>
            new InfinispanPartition(idx, Location(server), toJavaSet(segs), properties)
         }.toArray
      }
   }
   private def toJavaSet(s: Set[Integer]) = new util.HashSet[Integer](s)

   private def cut[A](l: Seq[A], parts: Int) = (0 until parts).map { i => l.drop(i).sliding(1, parts).flatten.toSet }.filter(_.nonEmpty)

}
