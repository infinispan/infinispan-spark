import sbt._
import sbt.Keys._
import sbtassembly._
import sbtassembly.AssemblyKeys._

import Deps._

object ProjectBuild extends Build {

   val extractServer = taskKey[Seq[File]]("Extract infinispan server")

   lazy val core = (project in file("."))
         .settings(commonSettings: _ *)
         .settings(Publishing.settings: _*)
         .settings(
            moduleName := "infinispan-spark",
            libraryDependencies ++= Seq(sparkCore, sparkStreaming, sparkSQL, sparkHive, hotRodClient, queryDSL, jcip,
                                        junit, scalaTest, scalaDMR, infinispanServerZip),
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
            resourceGenerators in Test <+= extractServer
         ).disablePlugins(sbtassembly.AssemblyPlugin)

   lazy val examples = (project in file("examples/twitter"))
         .dependsOn(core)
         .settings(commonSettings: _ *)
         .settings(
            libraryDependencies ++= Seq(twitter4jCore, sparkStreamingTwitter),
            assemblyJarName in assembly := "infinispan-spark-twitter.jar",
            assemblyMergeStrategy in assembly := {
               case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
               case PathList("META-INF", "DEPENDENCIES.txt") => MergeStrategy.discard
               case PathList(ps@_*) if ps.last == "UnusedStubClass.class" => MergeStrategy.discard
               case "features.xml" => MergeStrategy.first
               case x => val oldStrategy = (assemblyMergeStrategy in assembly).value
                  oldStrategy(x)
            }
         ).aggregate(core)

   def commonSettings = Seq(
      scalaVersion := "2.11.5",
      crossScalaVersions := Seq("2.10.4", "2.11.5"),
      libraryDependencies ++= Seq(sparkCore, sparkStreaming, sparkSQL),

      scalacOptions <++= scalaVersion map { v =>
         val baseFlags = Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-unchecked", "-Xfatal-warnings", "-Yno-adapted-args", "-Ywarn-dead-code")
         if (v.startsWith("2.10"))
            baseFlags
         else
            baseFlags ++ Seq("-Xlint:_,-nullary-unit", "-Ywarn-unused", "-Ywarn-unused-import")
      },
      resolvers ++= Seq(
          "JBoss Releases" at "https://repository.jboss.org/nexus/content/repositories/releases/",
          "JBoss Snapshots" at "https://repository.jboss.org/nexus/content/repositories/snapshots/"
      ),
      test in assembly := {},
      parallelExecution in Test := false,
      parallelExecution in Global := false
   )

}
