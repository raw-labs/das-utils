import SbtDASPlugin.autoImport.*

lazy val root = (project in file("."))
  .enablePlugins(SbtDASPlugin)
  .settings(
    repoNameSetting := "das-utils",
    libraryDependencies ++= Seq(
      // DAS
      "com.raw-labs" %% "das-server-scala" % "0.6.0" % "compile->compile;test->test",
      // ScalaTest for unit tests
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "mockito-5-12" % "3.2.19.0" % "test"),
    dependencyOverrides ++= Seq(
      "io.netty" % "netty-handler" % "4.1.118.Final"
    ))
