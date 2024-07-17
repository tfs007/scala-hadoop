name := "ScalaHadoopFileCompression"
version := "1.0"
scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "3.3.2",
  "org.apache.hadoop" % "hadoop-common" % "3.3.2",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.2",
  "org.scalatest" %% "scalatest" % "3.2.10" % "test"
)

assembly / mainClass := Some("FileCompressor")
assembly / assemblyJarName := "ScalaHadoopFileCompression.jar"