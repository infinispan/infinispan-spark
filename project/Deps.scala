import Versions._
import sbt._

object Deps {
   // Core deps
   val hotRodClient = "org.infinispan" % "infinispan-client-hotrod" % infinispanVersion
   val queryDSL = "org.infinispan" % "infinispan-query-dsl" % infinispanVersion
   val remoteQueryClient = "org.infinispan" % "infinispan-remote-query-client" % infinispanVersion
   val protoStream = "org.infinispan.protostream" % "protostream" % protoStreamVersion
   val jcip = "net.jcip" % "jcip-annotations" % jcipAnnotationsVersion
   val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
   val sparkSQL = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
   val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
   val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

   // Tests dependencies
   val junit = "junit" % "junit" % junitVersion % "test"
   val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
   val infinispanServerZip = "org.infinispan.server" % "infinispan-server-build" % infinispanVersion % "test" artifacts Artifact("infinispan-server-build", "zip", "zip")
   val scalaDMR = "org.jboss" %% "dmr-repl" % scalaDMRVersion % "test"
   val shrinkWrap = "org.jboss.shrinkwrap" % "shrinkwrap-depchain" % shrinkWrapVersion % "test"
   val infinispanCore = "org.infinispan" % "infinispan-core" % infinispanVersion
   val log4j = "log4j" % "log4j" % log4jVersion % "test"
   val sl4jbridge = "org.slf4j" % "slf4j-log4j12" % sl4jVersion % "test"

   // Demo deps
   val twitterHbc = "com.twitter" % "hbc-core" % twitterHbcVersion
   val playJson = "com.typesafe.play" % "play-json_2.11" % playJsonVersion
}
