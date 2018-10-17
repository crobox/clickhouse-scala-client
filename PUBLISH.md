from http://www.scala-sbt.org/release/docs/Using-Sonatype.html

The credentials for your Sonatype OSSRH account need to be stored somewhere safe (e.g. NOT in the repository). Common convention is a ~/.sbt/1.0/sonatype.sbt file with the following:

```
credentials += Credentials("Sonatype Nexus Repository Manager",
                           "oss.sonatype.org",
                           "<your username>",
                           "<your password>")
```

To release and publish a version to oss.sonatype for both scala 2.11 and scala 2.12 run:

```
sbt release 
```

To only publish a certain version after f.e. the tags has been build but your PGP was not correctly unlocked you can run

```
sbt ++2.11.12 publishSigned
```

To close and release the staging repository on Sonatype you can either go to the web interface or use  

```
sbt sonatypeRelease
```