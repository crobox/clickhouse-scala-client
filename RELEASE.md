# Releasing

Releases are driven entirely by git tags. `sbt-ci-release` derives the version from the tag via sbt-dynver — there is no
`version.sbt` to edit and no `sbt release` step to run.

## Cutting a release

```bash
git tag v1.3.0
git push origin v1.3.0
```

That is the whole procedure. Pushing a tag triggers
[`.github/workflows/release.yml`](.github/workflows/release.yml), which runs `sbt ci-release` to cross-build, sign and
publish `client`, `dsl` and `testkit` for every version in `crossScalaVersions`.

Between tags, `sbt-dynver` produces snapshot versions of the form `1.3.0+3-abc1234-SNAPSHOT`. Snapshot publishing is
disabled, so those are local-only.

Verify the result on Maven Central:

- https://mvnrepository.com/artifact/com.crobox.clickhouse/client_2.13
- https://mvnrepository.com/artifact/com.crobox.clickhouse/client_3

Central sync typically trails the publish by 15–30 minutes, so a 404 immediately afterwards is expected.

## Release notes

The generated notes are a list of PR titles, which does not tell a consumer what breaks. Write them by hand for anything
with user-visible consequences — changed SQL output, changed defaults, removed functions, dropped transitive
dependencies. See the [v1.3.0 notes](https://github.com/crobox/clickhouse-scala-client/releases/tag/v1.3.0) for the
shape: what changed, who it affects, and how to opt out.

## Credentials

`release.yml` needs four repository or organisation secrets:

| Secret | Purpose |
|---|---|
| `PGP_SECRET` | base64-encoded private key used to sign artifacts |
| `PGP_PASSPHRASE` | passphrase for that key |
| `SONATYPE_USERNAME` | Sonatype Central user token name |
| `SONATYPE_PASSWORD` | Sonatype Central user token |

The signing key is refreshed by
[`.github/workflows/refresh-signing-key.yml`](.github/workflows/refresh-signing-key.yml).

Nothing needs to be configured on a developer machine; releasing does not happen locally.

## If a release fails

The publish is not reliably one-shot — a transient Sonatype or Maven Central error can fail the run. Re-run the failed
job from the Actions tab. The tag does not need moving, and `ci-release` is safe to retry: a version that already
published will fail rather than overwrite.
