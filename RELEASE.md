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

Release the client is done using the following three steps:

```
1. Release Code
2. Publish Artifacts
3. Releese Artifacts 
```

## Release Code

To release and publish a version to oss.sonatype for both scala 2.12 and scala 2.13 run:

```
sbt release 
```
It's likely that *after* pushing all artifacts to the online repository you'll see an error complaining that the
tag already exists. That's ok.

## Publish Artifacts

To only publish a certain version after f.e. the tags has been build, but your PGP was not correctly unlocked you can
run

```
sbt publishSigned
```

If you want to publish only a single SCALA version, use `sbt ++3.3.1 publishSigned`

## Release Artifacts

To close and release the staging repository on Sonatype you can either go to the web interface or use

```
sbt sonatypeRelease
```

If something goes wrong and you want to cleanup/rollback, use `sbt sonatypeDropAll`

You can verify if all has been published correctly by visiting the following url:<br>
https://oss.sonatype.org/#nexus-search;quick~clickhouse%20crobox