import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "MovieLens",
    libraryDependencies += scalaTest % Test,
    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.1" % "provided",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-hive
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.1" % "provided"

  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
