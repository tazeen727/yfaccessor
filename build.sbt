name := "yfaccessor"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.jsoup" % "jsoup" % "1.8.3",
  "org.specs2" %% "specs2-core" % "3.7" % "test"
)

scalacOptions in Test ++= Seq("-Yrangepos")
