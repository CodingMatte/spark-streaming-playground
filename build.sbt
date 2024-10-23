name := "spark-streaming"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion = "3.4.0"
val postgresVersion = "42.2.2"
val cassandraConnectorVersion = "3.0.0"
val akkaVersion = "2.6.19"
val akkaHttpVersion = "10.2.10"
val twitter4jVersion = "4.0.7"
val kafkaVersion = "2.8.0"
val log4jVersion = "2.17.2"
val nlpLibVersion = "3.5.1"

resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  // streaming-kafka
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,

  // low-level integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,

  // akka
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,

  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,

  // postgres
  "org.postgresql" % "postgresql" % postgresVersion,

  // twitter
  "org.twitter4j" % "twitter4j-core" % twitter4jVersion,
  "org.twitter4j" % "twitter4j-stream" % twitter4jVersion,

  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,

  "edu.stanford.nlp" % "stanford-corenlp" % nlpLibVersion,
  "edu.stanford.nlp" % "stanford-corenlp" % nlpLibVersion classifier "models",

  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion
)

// Include Java source directories
Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "main" / "java"
Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "test" / "java"

// Resolve version conflict
libraryDependencySchemes += "org.scala-lang.modules" %% "scala-parser-combinators" % VersionScheme.EarlySemVer

// Force specific version of scala-parser-combinators
dependencyOverrides += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1"

// Java 17 compatibility
Compile / javacOptions ++= Seq("--release", "17")
Compile / scalacOptions ++= Seq("-target:jvm-17")

// Enable forking for run and test tasks to apply javaOptions
Compile / fork := true
Test / fork := true

// Add JVM options globally for the project
javaOptions ++= Seq(
  "--add-opens", "java.base/java.nio=ALL-UNNAMED",
  "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED"
)
