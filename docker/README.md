# OpenMessaging Benchmark Framework Docker

## Precondition

You need to install [Eclipse Temurin 21](https://adoptium.net/)
and set the JAVA_HOME environment variable to its installation directory.

## Building the image

You can use one of the Dockerfiles based on your needs:

- `Dockerfile` - requires local build first (this is the one CI builds)
- `Dockerfile.build` - builds inside Docker, no local Maven needed

### `Dockerfile`

Uses `eclipse-temurin:21` and takes `BENCHMARK_TARBALL` as an argument.
While using this Dockerfile, you will need to build the project locally **first**.

```bash
mvn install -DskipTests
export BENCHMARK_TARBALL=package/target/openmessaging-benchmark-<VERSION>-SNAPSHOT-bin.tar.gz
docker build -t openmessaging-benchmark:latest --build-arg BENCHMARK_TARBALL . -f docker/Dockerfile
```

### `Dockerfile.build`

Uses Maven to build the project inside Docker, then uses `eclipse-temurin:21` as runtime.
This Dockerfile has no local dependency (you do not need Maven installed locally).

```bash
docker build -t openmessaging-benchmark:latest . -f docker/Dockerfile.build
```

## Image layout

Both Dockerfiles unpack the distribution to `/opt/benchmark` in a **single** published
layer, via a build stage that is discarded. Keep it that way: extracting to `/` and then
relocating the tree with `mv` in a later instruction writes the whole ~247 MB
distribution into two layers, both of which get pushed and pulled.
