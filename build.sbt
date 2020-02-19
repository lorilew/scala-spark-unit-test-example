
scalaVersion     := "2.11.12"
version          := "0.1.0-SNAPSHOT"
organization     := "com.example"
organizationName := "example"
name             := "Scala Spark Unit Test Example"


resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"


libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.11"

