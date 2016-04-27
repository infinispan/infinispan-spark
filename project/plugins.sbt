resolvers += "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
resolvers += "bintray-spark-packages" at "http://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.5")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")
