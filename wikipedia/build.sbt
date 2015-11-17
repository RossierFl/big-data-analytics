name := "wikipedia"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core"      % "1.5.1" % "provided",
    "org.scalatest"    %  "scalatest_2.10"  % "2.2.5" % "test",
    "com.novocode"     %  "junit-interface" % "0.11"  % "test->default"
)
