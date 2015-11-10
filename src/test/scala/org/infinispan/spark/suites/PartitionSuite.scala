package org.infinispan.spark.suites

import java.net.{InetSocketAddress, SocketAddress}
import java.util.Properties

import org.apache.spark.Partition
import org.infinispan.client.hotrod.CacheTopologyInfo
import org.infinispan.spark.rdd.{InfinispanPartition, InfinispanRDD, PerServerSplitter}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConversions._
import scala.language.implicitConversions


class PartitionSuite extends FunSuite with Matchers {

   private val splitter = new PerServerSplitter

   test("Partition replicated caches") {
      runTest(numSegments = 75, numOwners = 4, numServers = 4)(partitions = 1)
      runTest(numSegments = 4, numOwners = 2, numServers = 2)(partitions = 1)
      runTest(numSegments = 75, numOwners = 2, numServers = 2)(partitions = 1)
      runTest(numSegments = 60, numOwners = 3, numServers = 3)(partitions = 1)

      runTest(numSegments = 40, numOwners = 3, numServers = 3)(partitions = 2)
      runTest(numSegments = 4, numOwners = 2, numServers = 2)(partitions = 2)
      runTest(numSegments = 75, numOwners = 4, numServers = 4)(partitions = 2)
      runTest(numSegments = 75, numOwners = 2, numServers = 2)(partitions = 2)
   }

   test("Partition distributed caches") {
      (1 to 4).foreach { owner =>
         (1 to owner).foreach { server =>
            (1 to 3).foreach { partition =>
               runTest(numSegments = 60, numOwners = owner, numServers = server)(partition)
            }
         }
      }
   }

   test("Uneven distribution") {
      val segmentsPerServer = Map(makeServer("s1", 0) -> Set[Integer](1, 2, 3, 4, 5, 6),
         makeServer("s2", 0) -> Set[Integer](7, 8), makeServer("s3", 0) -> Set[Integer](9, 0))

      runTest(10, segmentsPerServer)(partitions = 1)
      runTest(10, segmentsPerServer)(partitions = 2)

   }

   private def makeServer(host: String, port: Int): SocketAddress = InetSocketAddress.createUnresolved(host, port)

   private def split(topologyInfo: CacheTopologyInfo, partitionPerServer: Integer) = {
      val props = new Properties()
      props.put(InfinispanRDD.PartitionsPerServer, partitionPerServer)
      splitter.split(topologyInfo, props)
   }

   implicit def unwrap(p: Partition): InfinispanPartition = p.asInstanceOf[InfinispanPartition]

   def assertAllSegmentsPresent(partitions: Array[Partition], numSegments: Int) = {
      partitions.flatMap(_.segments).flatten.toSet shouldBe Set(0 to numSegments - 1: _*)
   }

   def assertIdxCrescent(partitions: Array[Partition]) = {
      val (res, _) = partitions.foldLeft((true, -1)) { case ((acc, prev), p) => (acc && (p.index > prev), p.index) }
      res shouldBe true
   }

   def assertNoDuplicateSegments(partitions: Array[Partition], numSegments: Int) = {
      partitions.flatMap(_.segments).flatten.toList.size shouldBe numSegments
   }

   def assertLocations(partitions: Array[Partition], numServers: Int, partitionsPerServer: Int) = {
      val locs = partitions.groupBy(_.location.address)
      locs.keys.size shouldBe numServers
      locs.values.flatten.size shouldBe numServers * partitionsPerServer
   }

   def runTest(numSegments: Int, segmentsPerServer: Map[SocketAddress, Set[Integer]])(partitions: Int) {
      runTest(numSegments, new CacheTopologyInfo {
         override def getSegmentsPerServer =
            mapAsJavaMap(segmentsPerServer.mapValues(setAsJavaSet))

         override def getNumSegments: Int = numSegments
      })(partitions)
   }

   private def runTest(numSegments: Int, numOwners: Int, numServers: Int)(partitions: Int) {
      val topology = createServerTopology(numSegments, numOwners, numServers)
      runTest(numSegments, topology)(partitions)
   }

   private def runTest(numSegments: Int, topology: CacheTopologyInfo)(partitions: Int) {
      val numServers = topology.getSegmentsPerServer.keySet.size
      val result = split(topology, partitions)
      result.length shouldBe numServers * partitions
      assertAllSegmentsPresent(result, numSegments)
      assertNoDuplicateSegments(result, numSegments)
      assertLocations(result, numServers, partitions)
      assertIdxCrescent(result)
   }

   private def reverse[K, V](m: Map[K, Set[V]]) =
      m.values.toSet.flatten.map(v => (v, m.keys.filter(m(_).contains(v)).toSet)).toMap

   private def createServerTopology(numSegments: Int, numOwners: Int, numServers: Int) = {
      val servers = (1 to numServers).map(i => makeServer(s"server$i", 0)).toList
      val serversStream = (Iterator continually servers).flatten
      val s = (for (i <- 0 to numSegments - 1) yield int2Integer(i) -> serversStream.take(numOwners).toSet).toMap
      new CacheTopologyInfo {
         override def getSegmentsPerServer = mapAsJavaMap(reverse(s).mapValues(setAsJavaSet))

         override def getNumSegments: Int = numSegments
      }
   }

}
