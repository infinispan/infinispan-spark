package org.infinispan.spark.test

import java.io.File
import java.net.InetAddress
import java.nio.file.Paths

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.jboss.as.controller.PathAddress
import org.jboss.as.controller.client.ModelControllerClient
import org.jboss.as.controller.client.helpers.ClientConstants._
import org.jboss.dmr.ModelNode

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._
import scala.util.{Failure, Success, Try}

/**
 * @author gustavonalle
 */
object CacheType extends Enumeration {
   val LOCAL = Value("local-cache")
   val REPLICATED = Value("replicated-cache")
   val DISTRIBUTED = Value("distributed-cache")
}

/**
 * A cluster of Infinispan Servers that can be managed together
 */
private[test] class Cluster private(size: Int, location: String) {
   private val _servers = mutable.ListBuffer[InfinispanServer]()
   val CacheContainer = "clustered"

   Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = Try(shutDown())
   })

   def startAndWait(duration: Duration) = {
      val servers = for (i <- 0 to size - 1) yield {
         new InfinispanServer(location, s"server$i", clustered = true, i * 1000)
      }
      _servers ++= servers
      val futureServers = _servers.map(s => Future(s.startAndWaitForCluster(_servers.size)))
      Await.ready(Future.sequence(futureServers), duration)
   }

   def shutDown() = _servers.par.foreach(_.shutDown())

   def getFirstServer = _servers.head

   def obtainCache[K, V](name: String, cacheType: CacheType.Value): RemoteCache[K, V] = {
      _servers.foreach(_.addCache(CacheContainer, name, cacheType))
      getFirstServer.obtainRemoteCache(name, CacheContainer, cacheType).asInstanceOf[RemoteCache[K, V]]
   }

}

/**
 * A remote infinispan server controlled by issuing native management operations
 */
private[test] class InfinispanServer(location: String, name: String, clustered: Boolean = false, portOffSet: Int = 0) {
   val BinFolder = "bin"
   val LaunchScript = "standalone.sh"
   val NameNodeConfig = "-Djboss.node.name"
   val LogDirConfig = "-Djboss.server.log.dir"
   val ClusteredConfig = "clustered.xml"
   val PortOffsetConfig = "-Djboss.socket.binding.port-offset"
   val StackConfig = "-Djboss.default.jgroups.stack=tcp"
   val Protocol = "http-remoting"
   val InfinispanSubsystem = "datagrid-infinispan"
   val Host = "localhost"
   val ShutDownOp = "shutdown"
   val ManagementPort = 9990
   val HotRodPort = 11222

   lazy val client = ModelControllerClient.Factory.create(Protocol, InetAddress.getByName(Host), getManagementPort)
   lazy val remoteCacheManager = new RemoteCacheManager(new ConfigurationBuilder().addServer().host(Host).port(getHotRodPort).pingOnStartup(true).build)

   private var launcher: Process = _

   Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = Try(shutDown())
   })

   def start() = {
      val logDir = Paths.get(location, "logs")
      val launch = Paths.get(location, BinFolder, LaunchScript)
      new File(launch.toString).setExecutable(true)
      val cmd = mutable.ListBuffer[String](Paths.get(location, BinFolder, LaunchScript).toString)
      if (clustered) {
         cmd += s"-c=$ClusteredConfig"
      }
      cmd += s"$NameNodeConfig=$name"
      cmd += s"$LogDirConfig=$logDir/$name"
      if (portOffSet > 0) {
         cmd += s"$PortOffsetConfig=$portOffSet"
      }
      cmd += StackConfig
      launcher = Process(cmd).run(new ProcessLogger {
         override def out(s: => String): Unit = {}

         override def err(s: => String): Unit = {}

         override def buffer[T](f: => T): T = f
      })
   }

   def startAndWaitForCluster(size: Int) = {
      start()
      waitForNumberOfMembers(size)
   }

   def startAndWaitForLocalCacheManager() = {
      start()
      waitForLocalCacheManager()
   }

   def shutDown(): Unit = {
      launcher.destroy()
      val op = new ModelNode
      op.get(OP).set(ShutDownOp)
      val res = client.execute(op)
      if (!(res.get(OUTCOME).asString() == SUCCESS)) {
         throw new Exception(s"Failure to stop server $name")
      }
      client.close()
   }

   private[this] object ModelNodeResult {

      trait ModelNodeResult[T] {
         def getValue(mn: ModelNode): T
      }

      implicit object ModelNodeResultString extends ModelNodeResult[String] {
         override def getValue(mn: ModelNode): String = mn.asString()
      }

      implicit object ModelNodeResultInt extends ModelNodeResult[Int] {
         override def getValue(mn: ModelNode): Int = mn.asInt()
      }

   }

   import ModelNodeResult._

   def getHotRodPort = if (portOffSet == 0) HotRodPort else ManagementPort + portOffSet

   private def getManagementPort = if (portOffSet == 0) ManagementPort else ManagementPort + portOffSet

   def waitForNumberOfMembers(members: Int): Unit =
      waitForServerAttribute[Int](s"/subsystem=$InfinispanSubsystem/cache-container=clustered", "cluster-size", _ == members)

   def waitForLocalCacheManager(): Unit =
      waitForServerAttribute[String](s"/subsystem=$InfinispanSubsystem/cache-container=local", "cache-manager-status", _ == "RUNNING")

   def addLocalCache(cacheName: String) = {
      addCache("local", cacheName, CacheType.LOCAL)
   }

   def obtainNonClusteredCache(name: String) = obtainRemoteCache(name, "local", CacheType.LOCAL)

   def obtainRemoteCache(name: String, cacheContainer: String, cacheType: CacheType.Value) = {
      val existent = cacheExists(name, cacheContainer, cacheType)
      if (!existent) addCache(cacheContainer, name, cacheType)
      remoteCacheManager.getCache(name)
   }

   def cacheExists(name: String, cacheContainer: String, cacheType: CacheType.Value): Boolean = {
      val caches = readAttribute[String](s"/subsystem=$InfinispanSubsystem/cache-container=$cacheContainer/", "defined-cache-names")
      caches.exists(_.contains(name))
   }

   def addCache(cacheContainer: String, cacheName: String, cacheType: CacheType.Value): Unit = {
      val exists = cacheExists(cacheName, cacheContainer, cacheType)
      if (!exists) {
         val pathAddress = PathAddress.pathAddress(SUBSYSTEM, InfinispanSubsystem)
                 .append("cache-container", cacheContainer)
                 .append(cacheType.toString, cacheName)
         val op: ModelNode = new ModelNode
         op.get(OP).set(ADD)
         op.get(OP_ADDR).set(pathAddress.toModelNode)
         op.get("start").set("EAGER")
         if (cacheType != CacheType.LOCAL) {
            op.get("mode").set("SYNC")
         }
         val resp: ModelNode = client.execute(op)
         if (!(SUCCESS == resp.get(OUTCOME).asString)) {
            throw new IllegalArgumentException(resp.asString)
         }
      }
   }

   @tailrec
   private def retry(command: => Boolean, numTimes: Int = 60, waitBetweenRetries: Int = 1000): Unit = {
      Try(command) match {
         case Success(true) =>
         case Success(false) if numTimes == 0 => throw new Exception("Timeout waiting for condition")
         case Success(false) =>
            Thread.sleep(waitBetweenRetries)
            retry(command, numTimes - 1, waitBetweenRetries)
         case Failure(e) if numTimes == 0 => throw e
         case Failure(e) if numTimes > 1 =>
            Thread.sleep(waitBetweenRetries)
            retry(command, numTimes - 1, waitBetweenRetries)
      }
   }

   private def readAttribute[T](path: String, name: String)(implicit ev: ModelNodeResult[T]): Option[T] = {
      val pathAddress = PathAddress.parseCLIStyleAddress(path)
      val op = new ModelNode
      op.get(OP).set(READ_ATTRIBUTE_OPERATION)
      op.get(OP_ADDR).set(pathAddress.toModelNode)
      op.get("name").set(name)
      val res = client.execute(op)
      if (res.get(OUTCOME).asString() == SUCCESS) {
         val modelNode = res.get(RESULT)
         Some(ev.getValue(modelNode))
      } else {
         None
      }
   }

   private def waitForServerAttribute[T](path: String, name: String, condition: T => Boolean)(implicit ev: ModelNodeResult[T]): Unit = {
      retry {
         val attribute = readAttribute[T](path, name)(ev)
         attribute.exists(condition)
      }
   }
}

object SingleNode {
   private val server: InfinispanServer = new InfinispanServer(getClass.getResource("/infinispan-server/").getPath, "server1")

   def start() = server.startAndWaitForLocalCacheManager()

   def shutDown() = server.shutDown()

   def getServerPort = server.getHotRodPort

   def getOrCreateCache(name: String) = server.obtainNonClusteredCache(name)
}

object Cluster {
   private val cluster: Cluster = new Cluster(3, getClass.getResource("/infinispan-server/").getPath)

   def start() = cluster.startAndWait(60 seconds)

   def shutDown() = cluster.shutDown()

   def getOrCreateCache(name: String, cacheType: CacheType.Value) = cluster.obtainCache(name, cacheType)

   def getFirstServerPort = cluster.getFirstServer.getHotRodPort
}
