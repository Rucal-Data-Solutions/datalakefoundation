// import sbt.internal.util.logic.Formula
import Dependencies._

ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / version          := "0.5.0-SNAPSHOT"
ThisBuild / organization     := "nl.rucal"
ThisBuild / organizationName := "Rucal Data Solutions"

lazy val root = (project in file("."))
  .settings(
    name := "datalakefoundation",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.3.2",
      "org.apache.spark" %% "spark-sql" % "3.3.2",
      "org.apache.spark" %% "spark-hive" % "3.3.2"
      // munit % Test
    ),
    libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "12.2.0.jre8",
    libraryDependencies += "io.delta" %% "delta-core" % "2.3.0"
  )