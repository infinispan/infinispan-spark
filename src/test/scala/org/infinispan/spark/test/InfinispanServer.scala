package org.infinispan.spark.test

import java.io.File
import java.nio.file.Paths

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.client.hotrod.{RemoteCache, RemoteCacheManager}
import org.jboss.as.controller.client.helpers.ClientConstants._
import org.jboss.dmr.repl.{Client, Response}
import org.jboss.dmr.scala.{ModelNode, _}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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
private[test] class Cluster(size: Int, location: String) {
   private val _servers = mutable.ListBuffer[InfinispanServer]()
   val CacheContainer = "clustered"
   @volatile private var started = false

   Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = Try(shutDown())
   })

   def isStarted = started

   def startAndWait(duration: Duration) = {
      val servers = for (i <- 0 to size - 1) yield {
         new InfinispanServer(location, s"server$i", clustered = true, i * 1000)
      }
      _servers ++= servers
      val futureServers = _servers.map(s => Future(s.startAndWaitForCluster(_servers.size)))
      val outcome = Future.sequence(futureServers)
      Await.ready(outcome, duration)
      outcome.value match {
         case Some(Failure(e)) => throw new RuntimeException(e)
         case _ => started = true
      }
   }

   def shutDown() = if (started) _servers.par.foreach(_.shutDown())

   def getFirstServer = _servers.head

   def obtainCache[K, V](name: String, cacheType: CacheType.Value, extraConfigs: Option[ModelNode]): RemoteCache[K, V] = {
      _servers.foreach(_.addCache(CacheContainer, name, cacheType, extraConfigs))
      getFirstServer.obtainRemoteCache(name).asInstanceOf[RemoteCache[K, V]]
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
   val TimeoutConfig = "-Djgroups.join_timeout=1000"
   val Protocol = "http-remoting"
   val InfinispanSubsystem = "datagrid-infinispan"
   val Host = "localhost"
   val ShutDownOp = "shutdown"
   val ManagementPort = 9990
   val HotRodPort = 11222
   @volatile private var started = false

   private lazy val serverHome = if (Paths.get(location).toFile.exists()) location
   else {
      Option(getClass.getResource(location)).map(_.getPath) match {
         case None => throw new IllegalArgumentException(s"Server not found in location $location.")
         case Some(c) => c
      }
   }
   val client = {
      new Client().connect(port = getManagementPort)
   }

   val DefaultCacheConfig = Map(
      "statistics" -> "true",
      "start" -> "eager"
   )

   lazy val remoteCacheManager = new RemoteCacheManager(new ConfigurationBuilder().addServer().host(Host).port(getHotRodPort).pingOnStartup(true).build)

   private var launcher: Process = _

   Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = Try(shutDown())
   })

   def start() = {
      val logDir = Paths.get(serverHome, "logs")
      val launch = Paths.get(serverHome, BinFolder, LaunchScript)
      new File(launch.toString).setExecutable(true)
      val cmd = mutable.ListBuffer[String](Paths.get(serverHome, BinFolder, LaunchScript).toString)
      if (clustered) {
         cmd += s"-c=$ClusteredConfig"
         cmd += StackConfig
         cmd += TimeoutConfig
      }
      cmd += s"$NameNodeConfig=$name"
      cmd += s"$LogDirConfig=$logDir/$name"
      if (portOffSet > 0) {
         cmd += s"$PortOffsetConfig=$portOffSet"
      }
      launcher = Process(cmd).run(new ProcessLogger {
         override def out(s: => String): Unit = {}

         override def err(s: => String): Unit = {}

         override def buffer[T](f: => T): T = f
      })
      started = true
   }

   def isStarted = started

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
      val res = client ! ModelNode(OP -> ShutDownOp)
      if (res.isFailure) {
         throw new Exception(s"Failure to stop server $name")
      }
      client.close()
      started = false
   }

   private[this] object ModelNodeResult {

      trait ModelNodeResult[T] {
         def getValue(mn: ModelNode): Option[T]
      }

      implicit object ModelNodeResultString extends ModelNodeResult[String] {
         override def getValue(mn: ModelNode) = mn.asString
      }

      implicit object ModelNodeResultInt extends ModelNodeResult[Int] {
         override def getValue(mn: ModelNode) = mn.asInt
      }

      implicit object ModelNodeResultUnit extends ModelNodeResult[Unit] {
         override def getValue(mn: ModelNode) = None
      }

   }

   import ModelNodeResult._

   def getHotRodPort = if (portOffSet == 0) HotRodPort else ManagementPort + portOffSet

   private def getManagementPort = if (portOffSet == 0) ManagementPort else ManagementPort + portOffSet

   def waitForNumberOfMembers(members: Int): Unit =
      waitForServerOperationResult[Int](ModelNode() at ("subsystem" -> InfinispanSubsystem)/ ("cache-container" -> "clustered") op 'read_attribute('name -> "cluster-size"), _ == members)

   def waitForLocalCacheManager(): Unit =
      waitForServerOperationResult[String](ModelNode() at ("subsystem" -> InfinispanSubsystem)/ ("cache-container" -> "local") op 'read_attribute('name -> "cache-manager-status"), _ == "RUNNING")

   def addLocalCache(cacheName: String, config: Option[ModelNode]) = {
      addCache("local", cacheName, CacheType.LOCAL, config)
   }

   def obtainRemoteCache(name: String) = remoteCacheManager.getCache(name)

   def cacheExists(name: String, cacheContainer: String, cacheType: CacheType.Value): Boolean = {
      val op = ModelNode() at ("subsystem" -> InfinispanSubsystem) / ("cache-container" -> cacheContainer) op 'read_attribute (
         'name -> "defined-cache-names"
      )
      val caches = executeOperation[String](op)
      caches.exists(_.contains(name))
   }

   def addCache(cacheContainer: String, cacheName: String, cacheType: CacheType.Value, config: Option[ModelNode]): Unit = {
      if (!cacheExists(cacheName, cacheContainer, cacheType)) {
         val params = if (cacheType != CacheType.LOCAL)
            createAddOpWithAttributes(DefaultCacheConfig + ("mode" -> "SYNC"))
         else
            createAddOpWithAttributes(DefaultCacheConfig)

         val cacheContainerPath = ("subsystem" -> InfinispanSubsystem) / ("cache-container" -> cacheContainer)
         val configPath = cacheContainerPath / ("configurations" -> "CONFIGURATIONS") / (s"$cacheType-configuration" -> cacheName)

         // Create empty cache configuration
         val ops = ListBuffer(ModelNode() at configPath op params)

         // Add custom config
         config.foreach(ops += createInsertOperations(configPath, _))

         // Create cache based on configuration
         ops += ModelNode() at cacheContainerPath / (cacheType.toString -> cacheName) op 'add ('configuration -> cacheName)

         executeOperation[Unit](ModelNode.composite(ops))
      }
   }

   private def toParam(m: Map[String, Any]) = m.toArray.map { case (k, v) => (Symbol(k), v) }

   private def createAddOpWithAttributes(m: Map[String, Any]): Operation = Operation('add)(toParam(m): _*)

   // Create optimal number of ops to add a model node to a specific path
   private def createInsertOperations(basePath: Address, modelNode: ModelNode): ModelNode = {
      val opsByPath = mutable.LinkedHashMap[ListBuffer[String],mutable.Map[String,Any]]()
      def process(n: ModelNode, path: ListBuffer[String]): Unit = {
         n.keys.foreach { k =>
            n(k) match {
               case v: ComplexModelNode => process(v, path :+ k)
               case v: ValueModelNode => opsByPath.getOrElseUpdate(path, mutable.Map()) += (k -> v)
            }
         }
      }
      process(modelNode, ListBuffer())
      val operations = opsByPath flatMap { case (path, attributes) =>
         val addOperation = createAddOpWithAttributes(attributes.toMap)
         val insertionPath = basePath
         if (path.isEmpty) {
            // Simple attribute of the cache
            attributes.map { case (attrName, attrValue) =>
               ModelNode() at insertionPath op 'write_attribute (
                  'name -> attrName,
                  'value -> attrValue
               )
            }
         } else if (path.size == 1) {
            // ModelNode attribute of the cache
            List(ModelNode() at insertionPath op 'write_attribute (
               'name -> path.head,
               'value -> modelNode(path.head)
            ))
         } else {
            // Nested model node inside the cache
            var nestedPath = insertionPath
            path.grouped(2).foreach(ar => nestedPath = nestedPath / (ar.head -> ar(1)))
            List(ModelNode() at nestedPath op addOperation)
         }
      }
      ModelNode.composite(operations)
   }

   private def executeOperation[T](op: ModelNode)(implicit ev: ModelNodeResult[T]): Option[T] = {
      client ! op match {
         case Success(node) =>
            node match {
               case Response(Response.Success, result) => ev.getValue(result)
               case Response(Response.Failure, failure) => throw new RuntimeException(s"Error executing operation $op: $failure")
            }
         case Failure(failure) => throw new RuntimeException(s"Error executing operation $op.toString", failure)
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

   private def waitForServerOperationResult[T](operation: ModelNode, condition: T => Boolean)(implicit ev: ModelNodeResult[T]): Unit = {
      retry {
         val result = executeOperation[T](operation)(ev)
         result.exists(condition)
      }
   }
}

object SingleNode {
   private val server: InfinispanServer = new InfinispanServer("/infinispan-server/", "server1")

   def start() = if (!server.isStarted) server.startAndWaitForLocalCacheManager()

   def shutDown() = server.shutDown()

   def getServerPort = server.getHotRodPort

   def getOrCreateCache(name: String, config: Option[ModelNode]) = {
      server.addLocalCache(name, config)
      server.obtainRemoteCache(name)
   }
}

object Cluster {
   val NumberOfServers = 3
   val ServerPath = "/infinispan-server/"
   val StartTimeout = 60 seconds

   private val cluster: Cluster = new Cluster(NumberOfServers, ServerPath)

   def start() = if (!cluster.isStarted) cluster.startAndWait(StartTimeout)

   def shutDown() = cluster.shutDown()

   def getOrCreateCache(name: String, cacheType: CacheType.Value, config: Option[ModelNode]) = cluster.obtainCache(name, cacheType, config)

   def getFirstServerPort = cluster.getFirstServer.getHotRodPort
}
