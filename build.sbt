import Deps._
import Versions._
import gigahorse.Request
import sbt.librarymanagement.Http

import scala.concurrent.Await
import scala.concurrent.duration._

val downloadServer = taskKey[Unit]("Download the server zip")
val extractServer = taskKey[Seq[File]]("Extract infinispan server")
lazy val getSparkVersion = taskKey[Unit]("Get Spark version used")
lazy val getInfinispanVersion = taskKey[Unit]("Get Infinispan version used")
lazy val ServerFolder = "infinispan-server"
lazy val serverZip = s"infinispan-server-$infinispanVersion.zip"
lazy val ServerZipURL = s"https://downloads.jboss.org/infinispan/$infinispanVersion/$serverZip"
lazy val ServerDownloadDir: String = System.getProperty("user.home")

Test / logBuffered := false

lazy val core = (project in file("."))
  .settings(commonSettings: _ *)
  .settings(Publishing.settings: _*)
  .settings(
    moduleName := "infinispan-spark",
    libraryDependencies ++= Seq(sparkCore, sparkStreaming, sparkSQL, sparkHive, hotRodClient, queryDSL, jcip,
      junit, scalaTest, remoteQueryClient, protoStream, jbossMarshalling,
      infinispanCommons, shrinkWrap, infinispanCore, sl4jbridge, log4j, sttp, uJson),
    downloadServer := {
      val destination = new File(ServerDownloadDir, serverZip)
      if (java.nio.file.Files.notExists(destination.toPath)) {
        println("Server not available locally, downloading...")
        val f = Http.http.download(Request(ServerZipURL), destination)
        Await.result(f, 5 minutes)
      } else {
        println("Server already downloaded")
      }
    },
    extractServer := {
      val d = downloadServer.value
      val report = update.value
      val zipPath = new File(ServerDownloadDir, serverZip)
      val destination = (resourceManaged in Test).value
      val destinationWithoutVersion = destination / ServerFolder
      IO.unzip(zipPath, destination).toSeq
      (destination ** "*infinispan-server*").get.head.renameTo(destinationWithoutVersion)
      installScalaLibrary(report, destinationWithoutVersion, scalaVersion.value)
      (destinationWithoutVersion ** AllPassFilter).get
    },
    getSparkVersion := {
      println(Versions.sparkVersion)
    },
    getInfinispanVersion := {
      println(Versions.infinispanVersion)
    },
    resourceGenerators in Test += extractServer,
    publishArtifact in Test := true,
    mappings in(Test, packageBin) ~= (_.filter(!_._1.getPath.contains("infinispan-server")))
  ).disablePlugins(sbtassembly.AssemblyPlugin, plugins.JUnitXmlReportPlugin).aggregate(LocalProject("examplesTwitter"), LocalProject("examplesSnippets"))

lazy val examples = project in file("examples")

lazy val examplesSnippets = (project in file("examples/snippets"))
  .dependsOn(core)
  .settings(commonSettings: _ *)
  .settings(
    scalacOptions --= Seq("-Ywarn-dead-code", "-Ywarn-unused"),
    publishLocal := {},
    publish := {}
  )

lazy val examplesTwitter = (project in file("examples/twitter"))
  .dependsOn(core)
  .settings(commonSettings: _ *)
  .settings(
    libraryDependencies ++= Seq(twitterHbc, playJson),
    assemblyJarName in assembly := "infinispan-spark-twitter.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", "DEPENDENCIES.txt") => MergeStrategy.discard
      case PathList(ps@_*) if ps.last == "UnusedStubClass.class" => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
      case "features.xml" => MergeStrategy.first
      case x => val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    publishLocal := {},
    publish := {}
  )

def commonSettings = Seq(
  scalaVersion := "2.12.8",
  libraryDependencies ++= Seq(sparkCore, sparkStreaming, sparkSQL),

  scalacOptions ++= {
    scalaVersion map { _ =>
      val baseFlags = Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-unchecked", "-Yno-adapted-args", "-Ywarn-dead-code")
      baseFlags ++ Seq("-Xlint:_,-nullary-unit", "-Ywarn-unused", "-Ywarn-unused-import")
    }
    }.value,

  resolvers ++= Seq(
    "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
    "JBoss Releases" at "https://repository.jboss.org/nexus/content/repositories/releases/",
    "JBoss Snapshots" at "https://repository.jboss.org/nexus/content/repositories/snapshots/"
  ),
  test in assembly := {},
  parallelExecution in Test := false,
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-u", s"${crossTarget.value.getAbsolutePath}/test-reports/", "-o"),
  updateOptions := updateOptions.value.withLatestSnapshots(false),
  parallelExecution in Global := false
)

def installScalaLibrary(report: UpdateReport, serverDir: File, version: String): Unit = {
  val scalaLibrary = report.matching(artifactFilter(name = "scala-library")).head
   IO.copyFile(scalaLibrary,  serverDir / "lib" / scalaLibrary.getName)
}
