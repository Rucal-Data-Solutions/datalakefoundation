ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / version          := "1.2.1"
ThisBuild / organization     := "nl.rucal"
ThisBuild / organizationName := "Rucal Data Solutions"
val sparkVersion = "3.5.1"

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
    ),
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.22.1" % "provided",
    libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.22.1" % "provided",

    libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "11.2.2.jre8" % "provided",
    libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0",
    


    javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED"
      ,"--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
      ,"--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
      ,"--add-opens=java.base/java.io=ALL-UNNAMED"
      ,"--add-opens=java.base/java.net=ALL-UNNAMED"
      ,"--add-opens=java.base/java.nio=ALL-UNNAMED"
      ,"--add-opens=java.base/java.util=ALL-UNNAMED"
      ,"--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
      ,"--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
      ,"--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
      ,"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
      ,"--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
      ,"--add-opens=java.base/sun.security.action=ALL-UNNAMED"
      ,"--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
      )
  )
