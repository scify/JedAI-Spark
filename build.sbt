organization := "org.scify"
name := "spark_er"
version := "1.0"
scalaVersion := "2.11.8"
val sparkVersion = "2.0.2"

unmanagedBase := baseDirectory.value / "custom_lib"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0" % Provided

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0" % Provided

// https://mvnrepository.com/artifact/org.apache.spark/spark-graphx_2.11
libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.1.0" % Provided

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0" % Provided

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.1.0" % Provided

// https://mvnrepository.com/artifact/com.twitter/algebird-core_2.11
//libraryDependencies += "com.twitter" % "algebird-core_2.11" % "0.12.3"

// https://mvnrepository.com/artifact/org.apache.commons/commons-math3
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

// https://mvnrepository.com/artifact/commons-codec/commons-codec
libraryDependencies += "commons-codec" % "commons-codec" % "1.11"

// https://mvnrepository.com/artifact/org.jgrapht/jgrapht-core
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "0.9.0"//% "1.0.1"

// https://mvnrepository.com/artifact/org.json/json
libraryDependencies += "org.json" % "json" % "20170516"

// https://mvnrepository.com/artifact/org.apache.livy/livy-core
libraryDependencies += "org.apache.livy" %% "livy-core" % "0.7.0-incubating"


assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
}
