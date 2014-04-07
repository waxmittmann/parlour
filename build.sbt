uniform.project("parlour", "au.com.cba.omnia.parlour")

resolvers ++= List(
  "conjars"  at "http://conjars.org/repo",
  "cloudera" at "https://repository.cloudera.com/cloudera/repo/"
)


libraryDependencies ++=
  depend.scaldingproject() ++
    depend.scalaz() ++ Seq(
      // The version of `commons-daemon` in Sqoop has a broken POM so another (fixed) version is included.
      "org.apache.sqoop"   % "sqoop"          % "1.4.3-cdh4.6.0" exclude("commons-daemon", "commons-daemon"),
      "commons-daemon"     % "commons-daemon" % "1.0.13"
    )

uniformAssemblySettings
