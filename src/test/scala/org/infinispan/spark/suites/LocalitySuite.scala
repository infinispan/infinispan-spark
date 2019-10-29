package org.infinispan.spark.suites

import java.net.{InetSocketAddress, SocketAddress}

import org.infinispan.spark.rdd.PreferredServerBalancingStrategy
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

class LocalitySuite extends FunSuite with Matchers {

   val servers: Set[SocketAddress] = Set(
      new InetSocketAddress("host1", 80),
      new InetSocketAddress("host2", 81),
      new InetSocketAddress("host3", 82)
   )

   test("start iteration on hinted server") {
      val preferred = new InetSocketAddress("host1", 80)

      val lbStrategy = new PreferredServerBalancingStrategy(preferred)
      lbStrategy.setServers(servers.asJava)

      lbStrategy.nextServer(Set[SocketAddress]().asJava) shouldBe preferred
      lbStrategy.nextServer(Set[SocketAddress](preferred).asJava) shouldNot be(preferred)
   }

   test("ignore invalid hinted server") {
      val invalid = new InetSocketAddress("invalidHost", 81)

      val lbStrategy = new PreferredServerBalancingStrategy(invalid)
      lbStrategy.setServers(servers.asJava)

      lbStrategy.nextServer(Set[SocketAddress](invalid).asJava) shouldNot be(invalid)
   }


}
