// import sbt.internal.util.logic.Formula
import Dependencies._

ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / version          := "0.8.1-SNAPSHOT"
ThisBuild / organization     := "nl.rucal"
ThisBuild / organizationName := "Rucal Data Solutions"
val sparkVersion = "3.4.1"

lazy val root = (project in file("."))
  .settings(
    name := "datalakefoundation",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
      // munit % Test
    ),
    libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "11.2.2.jre8",
    libraryDependencies += "io.delta" %% "delta-core" % "2.4.0"
  )
