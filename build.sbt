name := "scala-case-study"

version := "0.1"

scalaVersion := "2.13.5"

lazy val akkaVersion       = "2.6.14"
lazy val logbackVersion    = "1.2.3"
lazy val circeVersion      = "0.13.0"
lazy val guiceVersion      = "5.0.0"
lazy val kafkaVesion       = "2.8.0"
lazy val pureconfigVersion = "0.15.0"
lazy val scalatestVersion  = "3.1.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka"     %% "akka-actor-typed" % akkaVersion,
  "ch.qos.logback"        % "logback-classic"   % logbackVersion,
  "org.apache.kafka"      % "kafka-clients"     % kafkaVesion,
  "io.circe"              %% "circe-core"       % circeVersion,
  "io.circe"              %% "circe-generic"    % circeVersion,
  "io.circe"              %% "circe-parser"     % circeVersion,
  "com.github.pureconfig" %% "pureconfig"       % pureconfigVersion,
  // Test Dependencies
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion      % Test,
  "org.scalatest"     %% "scalatest"                % scalatestVersion % Test

)
