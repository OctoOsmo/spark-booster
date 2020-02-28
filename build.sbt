scalaVersion := "2.12.10"

name := "spark-booster"

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "3.0.0-preview2" % "provided"
  exclude
  ("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % "3.0.0-preview2" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "3.2.0" % "provided", "org.apache.hadoop" % "hadoop-aws" % "3.2.0")
