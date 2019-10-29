package org.infinispan.spark.test

import java.io.{File, FileWriter}
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{Files, Paths}

import org.infinispan.client.hotrod.RemoteCacheManager
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder
import org.infinispan.filter.{KeyValueFilterConverterFactory, NamedFactory}
import org.infinispan.spark.domain.{Person, Runner}
import org.infinispan.spark.test.TestingUtil.{DefaultDuration, waitForCondition}
import org.jboss.shrinkwrap.api.ShrinkWrap
import org.jboss.shrinkwrap.api.exporter.ZipExporter
import org.jboss.shrinkwrap.api.spec.JavaArchive
import ujson._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Try}

class FilterDef(val factoryClass: Class[_ <: KeyValueFilterConverterFactory[_, _, _]], val classes: Seq[Class[_]]) {
   val name: String = factoryClass.getAnnotation(classOf[NamedFactory]).name()
   val allClasses: Seq[Class[_]] = classes :+ factoryClass
}

class EntityDef(val classes: Seq[Class[_]], val jarName: String)

object TestEntities extends EntityDef(
   Seq(classOf[Runner], classOf[org.infinispan.spark.domain.Address], classOf[Person]), jarName = "entities.jar") {
   val moduleName: String = "deployment." + jarName
}

/**
  * A cluster of Infinispan Servers that can be managed together
  */
private[test] class Cluster(size: Int, location: String, cacheContainer: String) {
   private val _servers = mutable.ListBuffer[InfinispanServer]()
   private val _failed_servers = mutable.ListBuffer[InfinispanServer]()
   val ServerConfig = "infinispan.xml"
   @volatile private var started = false
   var entities: Option[EntityDef] = None
   var filters: ListBuffer[FilterDef] = ListBuffer[FilterDef]()

   Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = Try(shutDown())
   })

   def addEntities(entities: EntityDef): Unit = this.entities = Some(entities)

   def addFilter(filter: FilterDef): Unit = this.filters += filter

   private def startServers(servers: Seq[InfinispanServer], timeOut: Duration): Unit = {
      import scala.concurrent.blocking
      val futureServers = servers.map { s =>
         entities.foreach(s.addEntities)
         filters.foreach(s.addFilter)
         Future {
            blocking {
               s.startAndWaitForCluster(ServerConfig, cacheContainer, _servers.size + servers.size, timeOut)
            }
         }
      }
      val outcome = Future.sequence(futureServers)
      Await.ready(outcome, timeOut)
      outcome.value match {
         case Some(Failure(e)) => throw new RuntimeException(e)
         case _ =>
      }
   }

   private def startSequential(servers: Seq[InfinispanServer], timeOut: Duration): Unit = {
      Await.result(Future {
         servers.foreach { s =>
            entities.foreach(s.addEntities)
            s.startAndWaitForCacheManager(ServerConfig, _servers.size + servers.size, cacheContainer)
         }
      }, timeOut)
   }

   def isStarted: Boolean = started

   def startAndWait(duration: Duration, parallel: Boolean = true): Unit = {
      val servers = for (i <- 0 until size) yield {
         new InfinispanServer(location, s"server$i", clustered = true, false, cacheContainer, i * 1000)
      }
      if (parallel) startServers(servers, duration) else startSequential(servers, duration)
      _servers ++= servers
      started = true
   }

   def shutDown(): Unit = if (started) {
      _servers.par.foreach(_.shutDown())
      _servers.clear()
      started = false
   }

   def failServer(i: Int): Unit = if (i < _servers.size) {
      val failedServer = _servers.remove(i)
      failedServer.shutDown()
      _failed_servers += failedServer
   }

   def restoreFailed(timeOut: Duration): Unit = {
      startSequential(_failed_servers, timeOut)
      _servers ++= _failed_servers
      _failed_servers.clear()
   }

   def getFirstServer: InfinispanServer = _servers.head

   def createCache[K, V](name: String, extraConfigs: Option[String]): Unit = {
      _servers.foreach(_.addCache(name, extraConfigs))
   }

   def getServerList: String = _servers.map(s => s"localhost:${s.getHotRodPort}").mkString(";")

   def removeFilter(f: FilterDef): Boolean = _servers.head.removeFilter(f)

}

/**
  * A remote infinispan server controlled by issuing native management operations
  */
private[test] class InfinispanServer(location: String, name: String, clustered: Boolean = false, encrypted: Boolean, cacheContainer: String,
                                     portOffSet: Int = 0) {
   val BinFolder = "bin"
   val DeploymentFolder = "lib"
   val DefaultConfigFolder = "server/conf"
   val LaunchScript = "server.sh"
   val NameNodeConfig = "-Dinfinispan.node.name"
   val LogDirConfig = "-Dinfinispan.server.log.path"
   val PortOffsetConfig = "-Dinfinispan.socket.binding.port-offset"
   val StackConfig = "-Dinfinispan.cluster.stack=tcp"
   val PreferIpv4Config = "-Djava.net.preferIPv4Stack=true"
   val DataDir = "-Dinfinispan.server.data.path"
   val Host = "localhost"
   val Port = 11222
   val baseDebugPort = 8787
   @volatile private var started = false

   private lazy val serverHome = if (Paths.get(location).toFile.exists()) location
   else {
      Option(getClass.getResource(location)).map(_.getPath) match {
         case None => throw new IllegalArgumentException(s"Server not found in location $location, use './sbt test:compile' first.")
         case Some(c) => c
      }
   }

   val DefaultCacheConfig: String = {
      Js.Obj("distributed-cache" ->
        Js.Obj(
           "template" -> false,
           "statistics" -> true,
           "start" -> "eager",
           "owners" -> Js.Num(2),
           "mode" -> Js.Str("SYNC")
        )
      ).toString()
   }

   lazy val remoteCacheManager = new RemoteCacheManager(new ConfigurationBuilder().addServer().host(Host).port(getHotRodPort).build)

   private var launcher: Process = _

   lazy val client = InfinispanClient(Port + portOffSet, encrypted)

   Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = Try(shutDown())
   })

   def copyConfig(config: String): String = {
      val newConfig = s"$config-$name"
      val sourcePath = Paths.get(serverHome, DefaultConfigFolder, config)
      val destinationPath = Paths.get(serverHome, DefaultConfigFolder, newConfig)
      if (!destinationPath.toFile.exists) Files.copy(sourcePath, destinationPath, REPLACE_EXISTING)
      newConfig
   }

   def start(config: String): Unit = {
      val isDebug = Option(System.getProperty("serverDebug"))
      val infinispanTrace = Option(System.getProperty("enableTrace"))
      val logDir = Paths.get(serverHome, "logs")
      val dataDir = Paths.get(serverHome, "state")
      val launch = Paths.get(serverHome, BinFolder, LaunchScript)
      new File(launch.toString).setExecutable(true)
      val cmd = mutable.ListBuffer[String](Paths.get(serverHome, BinFolder, LaunchScript).toString)
      if (isDebug.isDefined) {
         cmd += s"--debug"
         cmd += s"${baseDebugPort + portOffSet}"
      }
      val serverConfig = copyConfig(config)
      val configFile = Paths.get(serverHome, DefaultConfigFolder, serverConfig).toFile
      XMLUtils.addCacheTemplate(cacheContainer, configFile)
      cmd += "-c"
      cmd += serverConfig
      setTrace(serverConfig, infinispanTrace)
      if (clustered) {
         cmd += StackConfig
         cmd += PreferIpv4Config
      }
      cmd += s"$NameNodeConfig=$name"
      cmd += s"$LogDirConfig=$logDir/$name"
      cmd += s"$DataDir=$dataDir/$name"
      if (portOffSet > 0) {
         cmd += s"$PortOffsetConfig=$portOffSet"
      }
      cmd += "-Dinfinispan.deserialization.whitelist.regexps=org.infinispan.spark.domain.*"
      launcher = Process(cmd).run(new ProcessLogger {
         override def out(s: => String): Unit = {}

         override def err(s: => String): Unit = println(s)

         override def buffer[T](f: => T): T = f
      })
      started = true
   }

   def isStarted: Boolean = started

   def startAndWaitForCluster(config: String, cacheManager: String, size: Int, duration: Duration): Unit = {
      start(config)
      waitForCacheManager(cacheManager, size, duration)
   }

   def startAndWaitForCacheManager(config: String, members: Int, name: String): Unit = {
      start(config)
      waitForCacheManager(name, members, DefaultDuration)
   }

   def shutDown(): Unit = {
      client.shutdown()
      launcher.exitValue()
      launcher.destroy()
      started = false
   }

   def getHotRodPort: Int = if (portOffSet == 0) Port else Port + portOffSet

   def waitForCacheManager(cacheManager: String, members: Int, duration: Duration): Unit = waitForCondition(() => client.isHealthy(cacheManager, members), duration)

   def addCache(cacheName: String, config: Option[String]): Unit = client.createCache(cacheName, config.getOrElse(DefaultCacheConfig))

   def deployArchive(archive: JavaArchive): Unit = {
      archive.as(classOf[ZipExporter]).exportTo(Paths.get(serverHome, DeploymentFolder, archive.getName).toFile, true)
   }

   def undeployArchive(name: String): Boolean = Paths.get(serverHome, DeploymentFolder, name).toFile.delete()

   def addFilter(filterDef: FilterDef): Unit = {
      deployArchive(ShrinkWrap
        .create(classOf[JavaArchive], s"${filterDef.name}.jar")
        .addClasses(filterDef.allClasses: _*)
        .addAsServiceProvider(classOf[KeyValueFilterConverterFactory[_, _, _]], filterDef.factoryClass))
   }


   def setTrace(configFile: String, categories: Option[String]): Unit = {
      val logConfig = Paths.get(serverHome, DefaultConfigFolder, "logging.properties")
      val file = logConfig.toFile

      def loadLoggingProperties = {
         val source = Source.fromFile(file)
         val stringses = source.getLines().map(l => {
            val keyValue = l.split("=")
            (keyValue(0), keyValue(1))
         }).toArray
         source.close()
         mutable.LinkedHashMap(stringses: _*)
      }

      def saveLoggingProperties(map: mutable.LinkedHashMap[String, String]): Unit = {
         val writer = new FileWriter(file)
         map.foreach { case (k,v) => writer.append(s"$k=$v\n")}
         writer.close()
      }

      val map = loadLoggingProperties
      categories.foreach(_.split(",").foreach(c => {
         map.+=(s"logger.$c.level" -> "TRACE")
         map("loggers") = map("loggers") + "," + c
      }))
      saveLoggingProperties(map)
   }

   def addEntities(entities: EntityDef): Unit = {
      if (entities.classes.nonEmpty) {
         deployArchive(ShrinkWrap.create(classOf[JavaArchive], entities.jarName).addClasses(entities.classes: _*))
      }
   }

   def removeFilter(filterDef: FilterDef): Boolean = undeployArchive(s"${filterDef.name}.jar")

}

sealed trait SingleNode {
   val ServerPath: String = "/infinispan-server/"
   val cacheContainer = "default"

   def isEncrypted: Boolean

   lazy val server: InfinispanServer = new InfinispanServer(ServerPath, "standalone", clustered = false, isEncrypted, cacheContainer)

   def getConfigFile: String

   def start(): Unit = {
      beforeStart()
      start(getConfigFile)
   }

   private def start(config: String): Unit = if (!server.isStarted) server.startAndWaitForCacheManager(config, 1, cacheContainer)

   def beforeStart(): Unit = {
      server.addEntities(TestEntities)
   }

   def afterShutDown(): Unit = {}

   def shutDown(): Unit = server.shutDown()

   def addFilter(f: FilterDef): Unit = server.addFilter(f)

   def removeFilter(f: FilterDef): Boolean = server.removeFilter(f)

   def getServerPort: Int = server.getHotRodPort

   def createCache(name: String, config: Option[String]): Unit = server.addCache(name, config)

}

object SingleStandardNode extends SingleNode {
   override def getConfigFile = "infinispan.xml"

   override def isEncrypted = false
}

object SingleSecureNode extends SingleNode {
   override def getConfigFile = "infinispan-secure.xml"

   override def isEncrypted = true

   val extraFiles: Seq[java.nio.file.Path] = Seq(toPath("/keystore_server.jks"), toPath("/ca.jks"))

   def toPath(classPath: String): java.nio.file.Path = Paths.get(getClass.getResource(classPath).getPath)

   override def beforeStart(): Unit = {
      super.beforeStart()
      val resource = getClass.getClassLoader.getResource(getConfigFile)
      val f = Paths.get(resource.toURI)
      val serverConfig = Paths.get(getClass.getResource(ServerPath).getPath, "server", "conf")
      Files.copy(f, serverConfig.resolve(getConfigFile), REPLACE_EXISTING)
      extraFiles.foreach(f => Files.copy(f, serverConfig.resolve(f.getFileName), REPLACE_EXISTING))
   }
}

object Cluster {
   private val NumberOfServers = 3
   private val ServerPath = "/infinispan-server/"
   private val StartTimeout = 200 seconds
   private val CacheContainer = "default"

   private val cluster: Cluster = new Cluster(NumberOfServers, ServerPath, CacheContainer)

   def start(): Unit = if (!cluster.isStarted) {
      cluster.addEntities(TestEntities)
      FilterDefs.list.foreach(cluster.addFilter)
      cluster.startAndWait(StartTimeout)
   }

   def addFilter[K, V, C](f: FilterDef): Unit = cluster.addFilter(f)

   def removeFilter(f: FilterDef): Boolean = cluster.removeFilter(f)

   def shutDown(): Unit = cluster.shutDown()

   def failServer(i: Int): Unit = cluster.failServer(i)

   def restore(): Unit = cluster.restoreFailed(StartTimeout)

   def createCache(name: String, config: Option[String]): Unit = cluster.createCache(name, config)

   def getFirstServerPort: Int = cluster.getFirstServer.getHotRodPort

   def getClusterSize: Int = cluster._servers.size

   def getServerList: String = cluster.getServerList

}
