import Deps._
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly._

object ProjectBuild extends Build {

   val extractServer = taskKey[Seq[File]]("Extract infinispan server")
   lazy val getSparkVersion = taskKey[Unit]("Get Spark version used")
   lazy val getInfinispanVersion = taskKey[Unit]("Get Infinispan version used")
   lazy val ServerFolder = "infinispan-server"

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
               val destinationWithoutVersion = destination / ServerFolder
               IO.unzip(zipPath, destination).toSeq
               (destination ** "*infinispan-server*").get.head.renameTo(destinationWithoutVersion)
               installScalaModule(report, destinationWithoutVersion, scalaVersion.value)
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
         ).disablePlugins(sbtassembly.AssemblyPlugin, plugins.JUnitXmlReportPlugin).aggregate(LocalProject("examplesTwitter"), LocalProject("examplesSnippets"))

   lazy val examplesRoot = project in file("examples")

   lazy val examplesSnippets = (project in file("examples/snippets"))
         .dependsOn(core)
         .settings(commonSettings: _ *)
         .settings(
             scalacOptions --= Seq("-Ywarn-dead-code","-Ywarn-unused"),
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
               case "features.xml" => MergeStrategy.first
               case x => val oldStrategy = (assemblyMergeStrategy in assembly).value
                  oldStrategy(x)
            },
            publishLocal := {},
            publish := {}
         )

   def commonSettings = Seq(
      scalaVersion := "2.11.8",
      libraryDependencies ++= Seq(sparkCore, sparkStreaming, sparkSQL),

      scalacOptions <++= scalaVersion map { v =>
         val baseFlags = Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-unchecked", "-Yno-adapted-args", "-Ywarn-dead-code")
         baseFlags ++ Seq("-Xlint:_,-nullary-unit", "-Ywarn-unused", "-Ywarn-unused-import")
      },
      resolvers ++= Seq(
          "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
          "JBoss Releases" at "https://repository.jboss.org/nexus/content/repositories/releases/",
          "JBoss Snapshots" at "https://repository.jboss.org/nexus/content/repositories/snapshots/"
      ),
      test in assembly := {},
      parallelExecution in Test := false,
      testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-u", s"${crossTarget.value.getAbsolutePath}/test-reports/", "-o"),
      parallelExecution in Global := false
   )


  def installScalaModule(report: UpdateReport, serverDir: File, version: String): Unit = {
    def moduleXML(scalaLibrary: String) = {

      <module xmlns="urn:jboss:module:1.3" name="org.scala">
        <resources>
          <resource-root path={s"$scalaLibrary"}/>
        </resources>
        <dependencies>
          <module name="sun.jdk"/>
        </dependencies>
      </module>

    }
    val moduleFile = "module.xml"
    val scalaLibrary = report.matching(artifactFilter(name = "scala-library")).head
    val moduleDir = serverDir / "modules" / "org" / "scala" / "main"

    IO.createDirectory(moduleDir)
    IO.write(moduleDir / moduleFile, moduleXML(scalaLibrary.getName).toString())
    IO.copyFile(scalaLibrary, moduleDir / scalaLibrary.getName)
  }

}
