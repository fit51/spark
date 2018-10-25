name := "spark-homework"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.3.2"
val scalatestVersion = "3.0.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion

libraryDependencies += "org.scalactic" %% "scalactic" % scalatestVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.10.0" % "test"

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
