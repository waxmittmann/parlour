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

strictDependencySettings

val thermometerVersion = "1.0.2-20150724061242-6a77919"

libraryDependencies ++=
  depend.hadoopClasspath ++
  depend.scaldingproject() ++
    depend.scalaz() ++ Seq(
      "au.com.cba.omnia"          %% "thermometer" % thermometerVersion % "test",
      noHadoop("org.scalikejdbc"  %% "scalikejdbc" % "2.1.2"            % "test"),
      noHadoop("org.apache.sqoop"  % "sqoop"       % "1.4.5-cdh5.2.4")
    )

uniformThriftSettings

parallelExecution in Test := false

updateOptions := updateOptions.value.withCachedResolution(true)

uniformAssemblySettings

uniform.docSettings("https://github.com/CommBank/parlour")

uniform.ghsettings

// No point having deprecation warnings since Sqoop forces us to use deprecated classes.
scalacOptions := scalacOptions.value.filter(_ != "-deprecation")
