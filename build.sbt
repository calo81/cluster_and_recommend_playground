name := "insurance_similarity"

version := "1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  jdbc,
  anorm,
  cache
)     

libraryDependencies += "org.apache.mahout" % "mahout-core" % "0.9"

libraryDependencies += "org.apache.mahout" % "mahout-integration" % "0.9"

libraryDependencies += "org.mongodb" % "mongo-java-driver" % "2.11.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"


play.Project.playScalaSettings
