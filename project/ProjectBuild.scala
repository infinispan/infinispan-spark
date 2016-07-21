import Deps._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly._

object ProjectBuild extends Build {

   val extractServer = taskKey[Seq[File]]("Extract infinispan server")
   lazy val getSparkVersion = taskKey[Unit]("Get Spark version used")
   lazy val getInfinispanVersion = taskKey[Unit]("Get Infinispan version used")

   lazy val core = (project in file("."))
         .settings(commonSettings: _ *)
         .settings(Publishing.settings: _*)
         .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
         .settings(
            moduleName := "infinispan-spark",
            libraryDependencies ++= Seq(sparkCore, sparkStreaming, sparkSQL, sparkHive, hotRodClient, queryDSL, jcip,
               junit, scalaTest, scalaDMR, remoteQueryClient, protoStream, infinispanServerZip,
               shrinkWrap, infinispanCore, sl4jbridge, log4j),
            extractServer := {
               val report = update.value
               val deps = report.matching(artifactFilter(name = "infinispan-server-build", extension = "zip"))
               val zipPath = deps.head.getAbsoluteFile
               val destination = (resourceManaged in Test).value
               val destinationWithoutVersion = destination / "infinispan-server"
               IO.unzip(zipPath, destination).toSeq
               (destination ** "*infinispan-server*").get.head.renameTo(destinationWithoutVersion)
               (destinationWithoutVersion ** AllPassFilter).get
            },
            getSparkVersion := {
               println(Versions.sparkVersion)
            },
            getInfinispanVersion := {
               println(Versions.infinispanVersion)
            },
            resourceGenerators in Test <+= extractServer,
            publishArtifact in Test := true,
            mappings in(Test, packageBin) ~= (_.filter(!_._1.getPath.contains("infinispan-server")))
         ).disablePlugins(sbtassembly.AssemblyPlugin).aggregate(examplesRef)

   lazy val examples = (project in file("examples/twitter"))
         .dependsOn(core)
         .settings(commonSettings: _ *)
         .settings(
            libraryDependencies ++= Seq(twitterHbc, playJson),
            assemblyJarName in assembly := "infinispan-spark-twitter.jar",
            assemblyMergeStrategy in assembly := {
               case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
               case PathList("META-INF", "DEPENDENCIES.txt") => MergeStrategy.discard
               case PathList(ps@_*) if ps.last == "UnusedStubClass.class" => MergeStrategy.discard
               case "features.xml" => MergeStrategy.first
               case x => val oldStrategy = (assemblyMergeStrategy in assembly).value
                  oldStrategy(x)
            },
            publishLocal := {},
            publish := {}
         )
   lazy val examplesRef = LocalProject("examples")

   def commonSettings = Seq(
      scalaVersion := "2.11.8",
      crossScalaVersions := Seq("2.10.6", "2.11.8"),
      libraryDependencies ++= Seq(sparkCore, sparkStreaming, sparkSQL),

      scalacOptions <++= scalaVersion map { v =>
         val baseFlags = Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-unchecked", "-Xfatal-warnings", "-Yno-adapted-args", "-Ywarn-dead-code")
         if (v.startsWith("2.10"))
            baseFlags
         else
            baseFlags ++ Seq("-Xlint:_,-nullary-unit", "-Ywarn-unused", "-Ywarn-unused-import")
      },
      resolvers ++= Seq(
          "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
          "JBoss Releases" at "https://repository.jboss.org/nexus/content/repositories/releases/",
          "JBoss Snapshots" at "https://repository.jboss.org/nexus/content/repositories/snapshots/"
      ),
      test in assembly := {},
      parallelExecution in Test := false,
      parallelExecution in Global := false
   )

}
