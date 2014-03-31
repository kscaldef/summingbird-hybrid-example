name := "summingbird-proto"

version := "0.0.1"

scalaVersion := "2.10.3"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-Yresolve-term-conflict:package"
)

val bijectionVersion = "0.6.2"
val storehausVersion = "0.9.0"
val tormentaVersion = "0.7.0"
val summingbirdVersion = "0.4.1"
val slf4jVersion = "1.6.6"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
  "log4j" % "log4j" % "1.2.16",
  "com.twitter" %% "bijection-netty" % bijectionVersion,
  "com.twitter" %% "storehaus-memcache" % storehausVersion,
  "com.twitter" %% "summingbird-core" % summingbirdVersion,
  "com.twitter" %% "summingbird-batch" % summingbirdVersion,
  "com.twitter" %% "summingbird-storm" % summingbirdVersion,
  "com.twitter" %% "summingbird-scalding" % summingbirdVersion,
  "com.twitter" %% "summingbird-client" % summingbirdVersion,
  "org.apache.storm" % "storm-core" % "0.9.1-incubating",
  // only works with kafta 0.7; implemented our own spout instead
  //"com.twitter" %% "tormenta-kafka" % tormentaVersion,
  "com.twitter" %% "tormenta-core" % tormentaVersion,
  // sigh... https://issues.apache.org/jira/browse/KAFKA-974
  "org.apache.kafka" %% "kafka" % "0.8.0"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
)

resolvers ++= Seq(
  Opts.resolver.sonatypeSnapshots,
  Opts.resolver.sonatypeReleases,
  "Clojars Repository" at "http://clojars.org/repo",
  "Conjars Repository" at "http://conjars.org/repo",
  "Twitter Maven" at "http://maven.twttr.com"
)
