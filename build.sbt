ThisBuild / scalaVersion     := "2.12.19"
ThisBuild / version          := "1.0.0-SNAPSHOT"
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
      "org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
      // munit % Test
    ),
    libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.18.0" % "provided",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.18.0" % "provided",

    libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "11.2.2.jre8",
    libraryDependencies += "io.delta" %% "delta-core" % "2.4.0" % "provided"
  )
