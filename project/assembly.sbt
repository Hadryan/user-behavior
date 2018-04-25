resolvers += Resolver.url("sbt-plugin-releases-scalasbt", url("http://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/"))

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")
