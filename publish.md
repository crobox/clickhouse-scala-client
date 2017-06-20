from http://www.scala-sbt.org/release/docs/Using-Sonatype.html

The credentials for your Sonatype OSSRH account need to be stored somewhere safe (e.g. NOT in the repository). Common convention is a ~/.sbt/0.13/sonatype.sbt file with the following:

credentials += Credentials("Sonatype Nexus Repository Manager",
                           "oss.sonatype.org",
                           "<your username>",
                           "<your password>")