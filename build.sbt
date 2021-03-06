organization  := "cf.s3"

version       := "0.1"

scalaVersion  := "2.10.3"

scalacOptions := Seq("-feature", "-unchecked", "-deprecation", "-encoding", "utf8")

resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/"
)

libraryDependencies ++= {
  val akkaV = "2.2.3"
  val sprayV = "1.2.0"
  Seq(
    "io.spray"            %   "spray-can"     % sprayV,
    "io.spray"            %   "spray-routing" % sprayV,
    "io.spray"            %   "spray-testkit" % sprayV,
    "com.typesafe.akka"   %%  "akka-actor"    % akkaV,
    "com.typesafe.akka"   %%  "akka-testkit"  % akkaV,
    "commons-codec"       % "commons-codec"   % "1.9",
    "org.specs2"          %% "specs2"         % "2.3.7" % "test",
    "org.scalatest"       % "scalatest_2.10"  % "2.0"   % "test"
  )
}

seq(Revolver.settings: _*)
