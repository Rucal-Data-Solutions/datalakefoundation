lazy val scala213 = "2.13.16"
lazy val supportedScalaVersions = List(scala213)

ThisBuild / scalaVersion     := scala213
ThisBuild / version          := "1.6.0"
ThisBuild / organization     := "nl.rucal"
ThisBuild / organizationName := "Rucal Data Solutions"
lazy val sparkVersion = "4.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "datalakefoundation",
    crossScalaVersions := supportedScalaVersions,

    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value % Provided,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value % Provided,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value % Provided
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-hive" % sparkVersion % Provided
    ),

    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test,
    libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.24.3" % Provided,
    libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.24.3" % Provided,

    libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "11.2.2.jre8" % Provided,
    libraryDependencies += "io.delta" %% "delta-spark" % "4.0.0" % Provided,

    // Make sure we have common-io for FileUtils in tests
    libraryDependencies += "commons-io" % "commons-io" % "2.13.0",

    Test / parallelExecution := false,
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )

  )
