# Versioning and releases

## Version source

`version.properties` is the single source of truth:

```properties
VERSION=1.7.0
```

`build.sbt` derives the build version from it:

| Build | Version | Set by |
|---|---|---|
| Local / CI (`sbt package`, `sbt test`) | `1.7.0-SNAPSHOT` | default |
| Nightly | `1.7.0-nightly.<yyyymmdd>.<sha7>` | `DLF_VERSION` in `scala.yml` |
| Stable release | `1.7.0` | `DLF_VERSION` in `release.yml` |

Building never modifies `version.properties`.

## Nightly builds

Every push to `main` that passes CI replaces the rolling **`nightly`** GitHub pre-release with a JAR of that commit. Nightlies are:

- marked as pre-release and never as *latest*;
- **not** published to Maven Central;
- untested beyond the unit tests. Use them to validate `main` on a real cluster before cutting a stable release.

Merging to `main` never produces a stable release.

## Stable release

1. Make sure `VERSION` in `version.properties` is the version you want to release and that `RELEASE_NOTES.md` is up to date. Change either through a normal PR.
2. Validate the current nightly on a real environment (Databricks / Fabric).
3. Go to **Actions → Release → Run workflow** on `main` and enter the same version, e.g. `1.7.0`.

The workflow:

1. refuses to run if it is not on `main`, if the input is not `x.y.z`, if it differs from `version.properties`, or if tag `v<version>` already exists;
2. runs the full test suite;
3. publishes the signed artifacts to Maven Central;
4. creates tag `v<version>` and a GitHub release with the JAR, marked as *latest*.

Publishing to Maven Central cannot be undone. If a later step fails after the publish succeeded, create the GitHub release by hand instead of re-running the workflow.

After the release, bump `VERSION` in `version.properties` (e.g. to `1.7.1` or `1.8.0`) in a PR so that nightlies show the next version.
