//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

uniform.project("parlour", "au.com.cba.omnia.parlour")

uniformDependencySettings

libraryDependencies ++=
  depend.scaldingproject() ++
    depend.scalaz() ++ Seq(
      // The version of `commons-daemon` in Sqoop has a broken POM so another (fixed) version is included.
      "org.apache.sqoop"   % "sqoop"          % "1.4.5-cdh5.2.4"
        exclude("commons-cli", "commons-cli")
        exclude("commons-collections", "commons-collections")
        exclude("commons-lang", "commons-lang")
        exclude("commons-io", "commons-io")
        exclude("commons-logging", "commons-logging")
        exclude("org.apache.hadoop", "hadoop-common")
        exclude("org.apache.hadoop", "hadoop-hdfs")
        exclude("org.apache.hadoop", "hadoop-mapreduce-client-core")
        exclude("org.apache.hadoop", "hadoop-mapreduce-client-common")
        exclude("org.apache.hbase", "hbase")
        exclude("org.apache.hcatalog", "hcatalog-core")
        exclude("hsqldb", "hsqldb")
        exclude("ant-contrib", "ant-contrib")
        exclude("org.apache.avro", "hadoop2")
        exclude("org.apache.avro", "avro-mapred")
        exclude("commons-daemon", "commons-daemon"),
      "commons-daemon"     % "commons-daemon" % "1.0.13",
      "au.com.cba.omnia"  %% "thermometer" % "0.7.0-20150318035217-23adcab" % "test",
      "org.scalikejdbc"   %% "scalikejdbc" % "2.1.2"                        % "test",
      "org.hsqldb"         % "hsqldb"      % "1.8.0.10"                     % "test"
    )

uniformThriftSettings

parallelExecution in Test := false

updateOptions := updateOptions.value.withCachedResolution(true)

uniformAssemblySettings

uniform.docSettings("https://github.com/CommBank/parlour")

uniform.ghsettings
