package org.infinispan.spark.rdd

import java.net.SocketAddress
import java.util
import org.slf4j.{Logger, LoggerFactory}

import org.infinispan.client.hotrod.impl.transport.tcp.{FailoverRequestBalancingStrategy, RoundRobinBalancingStrategy}

/**
  * Balancing strategy biased towards a pre-chosen server
  */
class PreferredServerBalancingStrategy(val preferredServer: SocketAddress) extends FailoverRequestBalancingStrategy {

   private lazy val logger: Logger = LoggerFactory.getLogger(classOf[PreferredServerBalancingStrategy])

   private val delegate = new RoundRobinBalancingStrategy

   override def nextServer(failedServers: util.Set[SocketAddress]): SocketAddress = {
      val nextServer = {
         if (Option(failedServers).exists(_.contains(preferredServer)) || !isServerValid) delegate.nextServer(failedServers)
         else preferredServer
      }
      logger.info(s"next server: $nextServer")
      nextServer
   }

   private def isServerValid = delegate.getServers.contains(preferredServer)

   override def setServers(servers: util.Collection[SocketAddress]): Unit = delegate.setServers(servers)
}


