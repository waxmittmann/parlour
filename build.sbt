uniform.project("parlour", "au.com.cba.omnia.parlour")

libraryDependencies ++=
  depend.scaldingproject() ++
    depend.scalaz() ++ Seq(
      // The version of `commons-daemon` in Sqoop has a broken POM so another (fixed) version is included.
      "org.apache.sqoop"   % "sqoop"          % "1.4.3-cdh4.6.0" exclude("commons-daemon", "commons-daemon"),
      "commons-daemon"     % "commons-daemon" % "1.0.13"
    )

uniformAssemblySettings
