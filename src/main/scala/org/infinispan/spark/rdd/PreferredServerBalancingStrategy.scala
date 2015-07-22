package org.infinispan.spark.rdd

import java.net.SocketAddress
import java.util

import org.apache.spark.Logging
import org.infinispan.client.hotrod.impl.transport.tcp.{FailoverRequestBalancingStrategy, RoundRobinBalancingStrategy}

/**
 * Balancing strategy biased towards a pre-chosen server
 */
class PreferredServerBalancingStrategy(val preferredServer: SocketAddress) extends FailoverRequestBalancingStrategy
with Logging {

   private val delegate = new RoundRobinBalancingStrategy

   override def nextServer(failedServers: util.Set[SocketAddress]): SocketAddress = {
      val nextServer = {
         if (Option(failedServers).exists(_.contains(preferredServer)) || !isServerValid) delegate.nextServer(failedServers)
         else preferredServer
      }
      logInfo(s"next server: $nextServer")
      nextServer
   }

   private def isServerValid = delegate.getServers.contains(preferredServer)

   override def setServers(servers: util.Collection[SocketAddress]): Unit = delegate.setServers(servers)

   override def nextServer(): SocketAddress = delegate.nextServer(null)
}


