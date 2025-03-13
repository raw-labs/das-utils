import java.nio.file.Paths

import sbt.*
import sbt.Keys.*

import com.typesafe.sbt.packager.docker.{Cmd, LayeredMapping}

// -----------------------------------------------------------------------------
// GitHub Packages credentials
// -----------------------------------------------------------------------------
ThisBuild / credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "raw-labs",
  sys.env.getOrElse("GITHUB_TOKEN", ""))

// -----------------------------------------------------------------------------
// Common & Build Settings
// -----------------------------------------------------------------------------
lazy val commonSettings = Seq(
  homepage := Some(url("https://www.raw-labs.com/")),
  organization := "com.raw-labs",
  organizationName := "RAW Labs SA",
  organizationHomepage := Some(url("https://www.raw-labs.com/")),
  updateOptions := updateOptions.in(Global).value.withCachedResolution(true),
  resolvers += "RAW Labs GitHub Packages" at "https://maven.pkg.github.com/raw-labs/_")

lazy val buildSettings = Seq(
  scalaVersion := "2.13.15",
  javacOptions ++= Seq("-source", "21", "-target", "21"),
  scalacOptions ++= Seq(
    "-feature",
    "-unchecked",
    "-deprecation",
    "-Xlint:-stars-align,_",
    "-Ywarn-dead-code",
    "-Ywarn-macros:after",
    "-Ypatmat-exhaust-depth",
    "160"))

lazy val compileSettings = Seq(
  Compile / doc / sources := Seq.empty,
  Compile / packageDoc / mappings := Seq(),
  Compile / packageSrc / publishArtifact := true,
  Compile / packageDoc / publishArtifact := false,
  Compile / packageBin / packageOptions += Package.ManifestAttributes(
    "Automatic-Module-Name" -> name.value.replace('-', '.')),
  compileOrder := CompileOrder.JavaThenScala)

// -----------------------------------------------------------------------------
// Test Settings
// -----------------------------------------------------------------------------
lazy val testSettings = Seq(
  Test / fork := true,
  Test / javaOptions ++= {
    import scala.collection.JavaConverters.*
    val props = System.getProperties
    props
      .stringPropertyNames()
      .asScala
      .filter(_.startsWith("raw."))
      .map(key => s"-D$key=${props.getProperty(key)}")
      .toSeq
  },
  Test / javaOptions ++= Seq(
    "-XX:+HeapDumpOnOutOfMemoryError",
    s"-XX:HeapDumpPath=${Paths.get(sys.env.getOrElse("SBT_FORK_OUTPUT_DIR", "target/test-results")).resolve("heap-dumps")}"),
  Test / publishArtifact := true)

// -----------------------------------------------------------------------------
// Publish Settings
// -----------------------------------------------------------------------------
val isCI = sys.env.getOrElse("CI", "false").toBoolean

lazy val publishSettings = Seq(
  versionScheme := Some("early-semver"),
  publish / skip := false,
  publishMavenStyle := true,
  // Point to a new GitHub repo for "das-utils", for example:
  publishTo := Some("GitHub raw-labs Apache Maven Packages" at "https://maven.pkg.github.com/raw-labs/das-utils"),
  publishConfiguration := publishConfiguration.value.withOverwrite(isCI))

// Consolidate everything into strict build settings
lazy val strictBuildSettings =
  commonSettings ++ compileSettings ++ buildSettings ++ testSettings ++ Seq(scalacOptions ++= Seq("-Xfatal-warnings"))

// -----------------------------------------------------------------------------
// Main Project: "das-utils"
// -----------------------------------------------------------------------------
lazy val root = (project in file("."))
  .settings(
    name := "das-utils",
    strictBuildSettings,
    publishSettings,
    libraryDependencies ++= Seq(
      // RAW Labs DAS & Protocol
      "com.raw-labs" %% "das-server-scala" % "0.5.0" % "compile->compile;test->test",
      "com.raw-labs" %% "protocol-das" % "1.0.0" % "compile->compile;test->test",
      // ScalaTest for unit tests
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % "test"))

// -----------------------------------------------------------------------------
// Docker Project: builds a Docker image for the DAS server
// -----------------------------------------------------------------------------
val amzn_jdk_version = "21.0.4.7-1"
val amzn_corretto_bin = s"java-21-amazon-corretto-jdk_${amzn_jdk_version}_amd64.deb"
val amzn_corretto_bin_dl_url = s"https://corretto.aws/downloads/resources/${amzn_jdk_version.replace('-', '.')}"

lazy val dockerSettings = strictBuildSettings ++ Seq(
  name := "das-utils-server",
  dockerBaseImage := s"--platform=amd64 debian:bookworm-slim",
  dockerLabels ++= Map(
    "vendor" -> "RAW Labs SA",
    "product" -> "das-utils-server",
    "image-type" -> "final",
    // Update the repo URL below to match your actual GitHub repository for "das-utils"
    "org.opencontainers.image.source" -> "https://github.com/raw-labs/das-utils"),
  Docker / daemonUser := "raw",
  dockerExposedVolumes := Seq("/var/log/raw"),
  dockerExposedPorts := Seq(50051),
  dockerEnvVars := Map("PATH" -> s"${(Docker / defaultLinuxInstallLocation).value}/bin:$$PATH"),
  // Remove the automatic USER 1001:0 set by sbt-native-packager
  dockerCommands := dockerCommands.value.filterNot {
    case Cmd("USER", args @ _*) => args.contains("1001:0")
    case _                      => false
  },
  dockerCommands ++= Seq(
    Cmd(
      "RUN",
      s"""set -eux \\
      && apt-get update \\
      && apt-get install -y --no-install-recommends \\
         curl wget ca-certificates gnupg software-properties-common fontconfig java-common \\
      && wget $amzn_corretto_bin_dl_url/$amzn_corretto_bin \\
      && dpkg --install $amzn_corretto_bin \\
      && rm -f $amzn_corretto_bin \\
      && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \\
         wget gnupg software-properties-common"""),
    Cmd("USER", "raw")),
  dockerEnvVars += "LANG" -> "C.UTF-8",
  dockerEnvVars += "JAVA_HOME" -> "/usr/lib/jvm/java-21-amazon-corretto",
  Compile / doc / sources := Seq.empty,
  Compile / packageDoc / mappings := Seq(),
  updateOptions := updateOptions.value.withLatestSnapshots(true),
  Linux / linuxPackageMappings += packageTemplateMapping(s"/var/lib/${packageName.value}")(),
  bashScriptDefines := {
    val ClasspathPattern = "declare -r app_classpath=\"(.*)\"\n".r
    bashScriptDefines.value.map {
      case ClasspathPattern(classpath) =>
        s"""
           |declare -r app_classpath="$${app_home}/../conf:$classpath"
           |""".stripMargin
      case entry => entry
    }
  },
  // Adjust layering: non-jar files or RAW jars go in the top layer
  Docker / dockerLayerMappings := (Docker / dockerLayerMappings).value.map {
    case lm @ LayeredMapping(Some(1), file, path) =>
      val fileName = java.nio.file.Paths.get(path).getFileName.toString
      if (!fileName.endsWith(".jar")) {
        LayeredMapping(Some(2), file, path)
      } else if (fileName.startsWith("com.raw-labs") && fileName.endsWith(".jar")) {
        LayeredMapping(Some(2), file, path)
      } else {
        lm
      }
    case lm => lm
  },
  // The default main class that runs the DAS server
  Compile / mainClass := Some("com.rawlabs.das.server.DASServer"),
  Docker / dockerAutoremoveMultiStageIntermediateImages := false,
  dockerAlias := dockerAlias.value.withTag(Option(version.value.replace("+", "-"))),
  // For multi-registry push (dev vs release)
  dockerAliases := {
    val devRegistry = sys.env.getOrElse("DEV_REGISTRY", "ghcr.io/raw-labs/das-utils")
    val releaseRegistry = sys.env.get("RELEASE_DOCKER_REGISTRY")
    val baseAlias = dockerAlias.value.withRegistryHost(Some(devRegistry))

    releaseRegistry match {
      case Some(releaseReg) => Seq(baseAlias, dockerAlias.value.withRegistryHost(Some(releaseReg)))
      case None             => Seq(baseAlias)
    }
  })

// -----------------------------------------------------------------------------
// Aggregate Docker Project
// -----------------------------------------------------------------------------
lazy val docker = (project in file("docker"))
  .dependsOn(root % "compile->compile;test->test")
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    strictBuildSettings,
    dockerSettings,
    libraryDependencies += "com.raw-labs" %% "das-server-scala" % "0.5.0" % "compile->compile;test->test")
