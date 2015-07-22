package org.infinispan.spark.suites

import java.net.{InetSocketAddress, SocketAddress}

import org.infinispan.spark.rdd.PreferredServerBalancingStrategy
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConversions._

class LocalitySuite extends FunSuite with Matchers {

   val servers = Set(
      new InetSocketAddress("host1", 80),
      new InetSocketAddress("host2", 81),
      new InetSocketAddress("host3", 82)
   )

   test("start iteration on hinted server") {
      val preferred = new InetSocketAddress("host1", 80)

      val lbStrategy = new PreferredServerBalancingStrategy(preferred)
      lbStrategy.setServers(servers)

      lbStrategy.nextServer(Set[SocketAddress]()) shouldBe preferred
      lbStrategy.nextServer(Set[SocketAddress](preferred)) shouldNot be(preferred)
   }

   test("ignore invalid hinted server") {
      val invalid = new InetSocketAddress("invalidHost", 81)

      val lbStrategy = new PreferredServerBalancingStrategy(invalid)
      lbStrategy.setServers(servers)

      lbStrategy.nextServer(Set[SocketAddress](invalid)) shouldNot be(invalid)
   }


}
