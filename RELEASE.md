from http://www.scala-sbt.org/release/docs/Using-Sonatype.html

The credentials for your Sonatype OSSRH account need to be stored somewhere safe (e.g. NOT in the repository).
Common convention is a ~/.sbt/1.0/sonatype.sbt or (~/.sbt/.credentials) file with the following:

```
credentials += Credentials("Sonatype Nexus Repository Manager",
                           "oss.sonatype.org",
                           "<your username>",
                           "<your password>")
```

If not done already, generate a key and upload it to a keyserver.
```
$ gpg --gen-key
$ gpg --list-secret-keys
$ gpg --keyserver keyserver.ubuntu.com --send-keys 2BE.......E804D85663F
```

The happy flow:
```
sbt release
sbt publishSigned
sbt sonatypeRelease 
```

To release and publish a version to oss.sonatype for both scala 2.12 and scala 2.13 run:

```
sbt release 
```

To only publish a certain version after f.e. the tags has been build, but your PGP was not correctly unlocked you can run

```
sbt publishSigned
sbt ++2.11.12 publishSigned
```

To close and release the staging repository on Sonatype you can either go to the web interface or use

```
sbt sonatypeRelease
sbt sonatypeDropAll
```

You can verify if all has been published correctly by visiting the following url:<br>
https://oss.sonatype.org/#nexus-search;quick~clickhouse%20crobox