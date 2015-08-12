import sbt.Keys._
import sbt._

object Publishing {

   lazy val scm =
      <scm>
         <url>http://github.com/infinispan/infinispan-spark</url>
         <connection>scm:git:git@github.com:infinispan/infinispan-spark.git</connection>
      </scm>

   lazy val developer =
      <developers>
         <developer>
            <id>gustavonalle</id>
            <name>Gustavo Fernandes</name>
         </developer>
      </developers>

   lazy val license =
      <licenses>
         <license>
            <name>Apache License</name>
            <url>http://www.apache.org/licenses/LICENSE-2.0</url>
            <distribution>repo</distribution>
         </license>
      </licenses>

   lazy val issues =
      <issueManagement>
         <system>jira</system>
         <url>https://issues.jboss.org/browse/ISPN</url>
      </issueManagement>

   val snapshotRepo = "JBoss Snapshot Repository" at "https://repository.jboss.org/nexus/content/repositories/snapshots"
   val releaseRepo = "JBoss Release Repository" at "https://repository.jboss.org/nexus/service/local/staging/deploy/maven2"

   lazy val credentialsSetting =
      credentials ++= (for {
         user <- sys.env.get("NEXUS_USER")
         pass <- sys.env.get("NEXUS_PASS")
      } yield Credentials("Sonatype Nexus Repository Manager", "repository.jboss.org", user, pass)).toSeq


   def settings: Seq[Setting[_]] = Seq(
      organization := "org.infinispan",
      organizationName := "JBoss, a division of Red Hat",
      organizationHomepage := Some(url("http://www.jboss.org")),
      description := "Infinispan Spark Connector",
      credentialsSetting,
      publishMavenStyle := true,
      publishTo <<= isSnapshot { (snapshot) => Some(if (snapshot) snapshotRepo else releaseRepo) },
      publishArtifact in Test := false,
      pomExtra := scm ++ developer ++ license ++ issues
   )


}