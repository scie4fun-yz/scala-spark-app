name := "scala-spark-app"
version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"

resolvers ++= Seq(
  "apache-snapshots" at "https://archive.apache.org/dist/spark/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion // ,
  //  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  //  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  //  "org.apache.spark" %% "spark-hive" % sparkVersion
)

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.8"

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)