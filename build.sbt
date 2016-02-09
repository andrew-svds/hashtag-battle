name := "hashtag-battle"

version := "1.0"

scalaVersion := "2.10.5"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.8.2.1"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
libraryDependencies += "org.mockito" % "mockito-core" % "1.10.19" % "test"
