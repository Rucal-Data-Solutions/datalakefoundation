import sbt._
import sbt.Keys._
import java.util.Properties
import java.io.{FileInputStream, FileOutputStream}

object VersionBump extends AutoPlugin {
  override def trigger = allRequirements

  private val versionPropsFile = file("version.properties")

  private def readProps(): Properties = {
    val props = new Properties()
    if (versionPropsFile.exists()) {
      val in = new FileInputStream(versionPropsFile)
      try props.load(in)
      finally in.close()
    }
    props
  }

  private def writeProps(props: Properties): Unit = {
    val out = new FileOutputStream(versionPropsFile)
    try props.store(out, null)
    finally out.close()
  }

  private def isStable: Boolean =
    sys.env.getOrElse("STABLE", "false").equalsIgnoreCase("true")

  private def computeVersion(): String = {
    val props = readProps()
    val base = props.getProperty("BASE_VERSION", "0.1")
    val build = props.getProperty("BUILD_NUMBER", "0").toInt
    val suffix = if (isStable) "" else "-SNAPSHOT"
    s"$base.$build$suffix"
  }

  private def bumpAndComputeVersion(): String = {
    val props = readProps()
    val base = props.getProperty("BASE_VERSION", "0.1")
    val build = props.getProperty("BUILD_NUMBER", "0").toInt + 1
    props.setProperty("BUILD_NUMBER", build.toString)
    writeProps(props)
    val suffix = if (isStable) "" else "-SNAPSHOT"
    s"$base.$build$suffix"
  }

  private val bumpVersionCommand =
    Command.command("bumpVersion") { state =>
      val newVersion = bumpAndComputeVersion()
      state.log.info(s"Version bumped to $newVersion")
      val extracted = Project.extract(state)
      extracted.appendWithSession(
        Seq(ThisBuild / version := newVersion),
        state
      )
    }

  override def buildSettings: Seq[Setting[?]] = Seq(
    version := computeVersion()
  )

  override def globalSettings: Seq[Setting[?]] = Seq(
    commands += bumpVersionCommand
  )
}
