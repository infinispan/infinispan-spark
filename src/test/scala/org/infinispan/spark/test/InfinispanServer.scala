package org.infinispan.spark.test

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{Files, Paths}

import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.filter.{KeyValueFilterConverterFactory, NamedFactory}
import org.infinispan.spark.test.TestingUtil.{DefaultDuration, waitForCondition}
import org.jboss.as.controller.client.helpers.ClientConstants._
import org.jboss.dmr.repl.{Client, Response}
import org.jboss.dmr.scala.{ModelNode, _}
import org.jboss.shrinkwrap.api.ShrinkWrap
import org.jboss.shrinkwrap.api.asset.StringAsset
import org.jboss.shrinkwrap.api.exporter.ZipExporter
import org.jboss.shrinkwrap.api.spec.JavaArchive

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
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

class FilterDef(val factoryImpl: Class[_ <: KeyValueFilterConverterFactory[_, _, _]], val deps: Class[_]*) {
   val name = factoryImpl.getAnnotation(classOf[NamedFactory]).name()
   val allClasses = deps :+ factoryImpl
}
/**
 * A cluster of Infinispan Servers that can be managed together
 */
private[test] class Cluster(size: Int, location: String) {
   private val _servers = mutable.ListBuffer[InfinispanServer]()
   private val _failed_servers = mutable.ListBuffer[InfinispanServer]()
   val CacheContainer = "clustered"
   @volatile private var started = false

   Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = Try(shutDown())
   })

   def startServers(servers: Seq[InfinispanServer], timeOut: Duration): Boolean = {
      val futureServers = servers.map(s => Future(s.startAndWaitForCluster(_servers.size + servers.size, timeOut)))
      val outcome = Future.sequence(futureServers)
      Await.ready(outcome, timeOut)
      val running = outcome.value match {
         case Some(Failure(e)) => throw new RuntimeException(e)
         case _ => true
      }
      running
   }

   def isStarted = started

   def startAndWait(duration: Duration) = {
      val servers = for (i <- 0 until size) yield {
         new InfinispanServer(location, s"server$i", clustered = true, i * 1000)
      }
      if (startServers(servers, duration)) {
         _servers ++= servers
         started = true
      }
   }

   def shutDown() = if (started) {
      _servers.par.foreach(_.shutDown())
      _servers.clear()
      started = false
   }

   def failServer(i: Int) = if (i < _servers.size) {
      val failedServer = _servers.remove(i)
      failedServer.shutDown()
      _failed_servers += failedServer
   }

   def restoreFailed(timeOut: Duration) = if (startServers(_failed_servers, timeOut)) {
      _servers ++= _failed_servers
      _failed_servers.clear()
   }

   def getFirstServer = _servers.head

   def createCache[K, V](name: String, cacheType: CacheType.Value, extraConfigs: Option[ModelNode]): Unit = {
      _servers.foreach(_.addCache(CacheContainer, name, cacheType, extraConfigs))
   }

   def getServerList = _servers.map( s => s"localhost:${s.getHotRodPort}").mkString(";")

   def addFilter(f: FilterDef) = _servers.foreach(_.addFilter(f))

   def removeFilter(f: FilterDef) = _servers.head.removeFilter(f)

}

/**
 * A remote infinispan server controlled by issuing native management operations
 */
private[test] class InfinispanServer(location: String, name: String, clustered: Boolean = false, portOffSet: Int = 0) {
   val BinFolder = "bin"
   val DeploymentFolder = "standalone/deployments"
   val DefaultConfigFolder = "standalone/configuration"
   val LaunchScript = "standalone.sh"
   val NameNodeConfig = "-Djboss.node.name"
   val LogDirConfig = "-Djboss.server.log.dir"
   val ClusteredConfigDefault = "clustered.xml"
   val ClusteredConfig = s"clustered-$name.xml"
   val PortOffsetConfig = "-Djboss.socket.binding.port-offset"
   val StackConfig = "-Djboss.default.jgroups.stack=tcp"
   val TimeoutConfig = "-Djgroups.join_timeout=1000"
   val DataDir = "-Djboss.server.data.dir"
   val Protocol = "http-remoting"
   val InfinispanSubsystem = "datagrid-infinispan"
   val Host = "localhost"
   val ShutDownOp = "shutdown"
   val ManagementPort = 9990
   val HotRodPort = 11222
   val baseDebugPort = 8787
   @volatile private var started = false

   private lazy val serverHome = if (Paths.get(location).toFile.exists()) location
   else {
      Option(getClass.getResource(location)).map(_.getPath) match {
         case None => throw new IllegalArgumentException(s"Server not found in location $location, use './sbt test:compile' first.")
         case Some(c) => c
      }
   }
   val client = new Client()

   val DefaultCacheConfig = Map(
      "statistics" -> "true",
      "start" -> "eager",
      "template" -> "false",
      "owners" -> "2"
   )

   lazy val remoteCacheManager = new RemoteCacheManager(new ConfigurationBuilder().addServer().host(Host).port(getHotRodPort).build)

   private var launcher: Process = _

   Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = Try(shutDown())
   })

   def copyConfig = {
      val sourcePath = Paths.get(serverHome, DefaultConfigFolder, ClusteredConfigDefault)
      val destinationPath = Paths.get(serverHome, DefaultConfigFolder, ClusteredConfig)
      if (!destinationPath.toFile.exists) Files.copy(sourcePath, destinationPath, REPLACE_EXISTING)
   }

   def start() = {
      val args = ManagementFactory.getRuntimeMXBean.getInputArguments
      val isDebug = args.contains("-Dserver-debug")
      val logDir = Paths.get(serverHome, "logs")
      val dataDir = Paths.get(serverHome, "state")
      val launch = Paths.get(serverHome, BinFolder, LaunchScript)
      new File(launch.toString).setExecutable(true)
      val cmd = mutable.ListBuffer[String](Paths.get(serverHome, BinFolder, LaunchScript).toString)
      if(isDebug) {
         cmd += s"--debug"
         cmd += s"${baseDebugPort + portOffSet}"
      }
      if (clustered) {
         copyConfig
         cmd += s"-c=$ClusteredConfig"
         cmd += StackConfig
         cmd += TimeoutConfig
      }
      cmd += s"$NameNodeConfig=$name"
      cmd += s"$LogDirConfig=$logDir/$name"
      cmd += s"$DataDir=$dataDir/$name"
      if (portOffSet > 0) {
         cmd += s"$PortOffsetConfig=$portOffSet"
      }
      launcher = Process(cmd).run(new ProcessLogger {
         override def out(s: => String): Unit = {}

         override def err(s: => String): Unit = {}

         override def buffer[T](f: => T): T = f
      })
      client.connect(port = getManagementPort)
      started = true
   }

   def isStarted = started

   def startAndWaitForCluster(size: Int, duration: Duration) = {
      start()
      waitForCacheManager("clustered")
      waitForNumberOfMembers(size, duration)
   }

   def startAndWaitForCacheManager(name: String) = {
      start()
      waitForCacheManager(name)
   }

   def shutDown(): Unit = {
      val res = client ! ModelNode(OP -> ShutDownOp)
      if (res.isFailure) {
         throw new Exception(s"Failure to stop server $name")
      }
      client.close()
      launcher.exitValue()
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

      implicit object ModelNodeResultBoolean extends ModelNodeResult[Boolean] {
         override def getValue(mn: ModelNode) = mn.asBoolean
      }

      implicit object ModelNodeResultUnit extends ModelNodeResult[Unit] {
         override def getValue(mn: ModelNode) = None
      }

   }

   import ModelNodeResult._

   def getHotRodPort = if (portOffSet == 0) HotRodPort else HotRodPort + portOffSet

   private def getManagementPort = if (portOffSet == 0) ManagementPort else ManagementPort + portOffSet

   def waitForNumberOfMembers(members: Int, duration: Duration = DefaultDuration): Unit =
      waitForServerOperationResult[Int](ModelNode() at ("subsystem" -> InfinispanSubsystem)/ ("cache-container" -> "clustered") op 'read_attribute('name -> "cluster-size"), _ == members, duration)

   def waitForCacheManager(name: String): Unit =
      waitForServerOperationResult[String](ModelNode() at ("subsystem" -> InfinispanSubsystem)/ ("cache-container" -> name) op 'read_attribute('name -> "cache-manager-status"), _ == "RUNNING")

   def addLocalCache(cacheName: String, config: Option[ModelNode]) = {
      addCache("local", cacheName, CacheType.LOCAL, config)
   }

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
         val configPath = cacheContainerPath / ("configurations" -> "CONFIGURATIONS") / (s"$cacheType-configuration" -> s"$cacheName")

         // Create empty cache configuration
         val ops = ListBuffer(ModelNode() at configPath op params)

         // Add custom config
         config.foreach(ops += createInsertOperations(configPath, _))

         // Create cache based on configuration
         ops += ModelNode() at cacheContainerPath / (cacheType.toString -> cacheName) op 'add ('configuration -> s"$cacheName")

         executeOperation[Unit](ModelNode.composite(ops))
      }
   }

   def deployArchive(archive: JavaArchive) = {
      archive.as(classOf[ZipExporter]).exportTo(Paths.get(serverHome, DeploymentFolder, archive.getName).toFile, true)
      waitForServerOperationResult[Boolean](
         ModelNode() at ("deployment" -> archive.getName) op 'read_attribute (
            'name -> "enabled"), _ == true
         )
   }

   def undeployArchive(name: String) = Paths.get(serverHome, DeploymentFolder, name).toFile.delete()

   def addFilter(filterDef: FilterDef) = {
      deployArchive(ShrinkWrap
              .create(classOf[JavaArchive], s"${filterDef.name}.jar")
              .addClasses(filterDef.allClasses: _*)
              .setManifest(new StringAsset("Dependencies: org.scala-lang.library"))
              .addAsServiceProvider(classOf[KeyValueFilterConverterFactory[_, _, _]], filterDef.factoryImpl))
   }

   def removeFilter(filterDef: FilterDef) = undeployArchive(s"${filterDef.name}.jar")

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

   private def waitForServerOperationResult[T](operation: ModelNode, condition: T => Boolean, duration: Duration = DefaultDuration)(implicit ev: ModelNodeResult[T]): Unit = {
      waitForCondition(() => {
         val result = executeOperation[T](operation)(ev)
         result.exists(condition)
      }, duration)
   }
}

object SingleNode {
   private val server: InfinispanServer = new InfinispanServer("/infinispan-server/", "standalone")

   def start() = if (!server.isStarted) server.startAndWaitForCacheManager("local")

   def shutDown() = server.shutDown()

   def addFilter(f: FilterDef) = server.addFilter(f)

   def removeFilter(f: FilterDef) = server.removeFilter(f)

   def getServerPort = server.getHotRodPort

   def createCache(name: String, config: Option[ModelNode]) = server.addLocalCache(name, config)

}

object Cluster {
   val NumberOfServers = 3
   val ServerPath = "/infinispan-server/"
   val StartTimeout = 200 seconds

   private val cluster: Cluster = new Cluster(NumberOfServers, ServerPath)

   def start() = if (!cluster.isStarted) cluster.startAndWait(StartTimeout)

   def addFilter[K,V,C](f: FilterDef) = cluster.addFilter(f)

   def removeFilter(f: FilterDef) = cluster.removeFilter(f)

   def shutDown() = cluster.shutDown()

   def failServer(i: Int) = cluster.failServer(i)

   def restore() = cluster.restoreFailed(StartTimeout)

   def createCache(name: String, cacheType: CacheType.Value, config: Option[ModelNode]) = cluster.createCache(name, cacheType, config)

   def getFirstServerPort = cluster.getFirstServer.getHotRodPort

   def getClusterSize = cluster._servers.size

   def getServerList = cluster.getServerList

}
