import Dependencies._

ThisBuild / scalaVersion := "2.12.8"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "flwr",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "com.h2database" % "h2" % "1.4.199",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
    )
  )
val sparkVersion = "2.4.3"

val javaVersion = "1.8"

run / javaOptions ++= Seq("-Xmx4G", "-Xms1G", "-XX:+CMSClassUnloadingEnabled")
run / fork := true
run / connectInput := true
outputStrategy := Some(StdoutOutput)
run / logLevel := util.Level.Error
run / showTiming := true
