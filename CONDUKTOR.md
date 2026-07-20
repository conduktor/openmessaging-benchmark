# Conduktor fork of the OpenMessaging Benchmark

Conduktor's soft fork of <https://github.com/openmessaging/benchmark>, consumed by the
Conduktor Gateway benchmark harness (`perf-tools/benchmarks`, `omb/` suite — see its
`omb/VENDOR.md` for how the image is built). This is the same public fork the team uses
for other gateway work; the `conduktor` branch below is the OMB-benchmark-engine line.

## Branches

- `conduktor` — current upstream tip (`5b1fa70`) + the commits below. The harness pins a
  commit of this branch. This is the branch this file documents.
- Other branches (`conduktor-gateway` [default], `master`, team feature branches) are
  pre-existing and unrelated to the benchmark-engine line — do not rebase or delete them.

## Divergence policy

Engine-internal fixes and diagnostics only. Payload generation, schema-registry framing and
all harness features live OUT of tree (`omb/interceptor/`, `omb/payload-generator/` in the
benchmarks repo). Fixes that make sense upstream are PR'd directly to upstream from this
public fork; when merged, we rebase and drop them here.

## Current divergence from upstream master

1. `docker/Dockerfile.build` works outside a git checkout (skips the spotless ratchet and
   other style/static-analysis checks) and caches `~/.m2`. Tests stay on.
2. `RateController` holds the offered rate until the producer warms up
   (`WARMUP_RATE_FRACTION`, default 0.5; 0 = legacy). Fixes the `producerRate: 0` finder
   collapsing to ~1 msg/s when a startup window is mistaken for saturation.
3. `RateController` logs the finder trajectory at INFO (`FINDER-TRACE`) so it is visible
   under the default worker log config.
