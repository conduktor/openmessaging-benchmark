#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM eclipse-temurin:21 AS dist

ARG BENCHMARK_TARBALL

# Using ADD instead of COPY because ${BENCHMARK_TARBALL} is an archive that needs to be extracted.
# This stage is never published, so the intermediate copy it creates costs nothing at pull time.
ADD ${BENCHMARK_TARBALL} /dist/

RUN mv /dist/openmessaging-benchmark-* /dist/benchmark


FROM eclipse-temurin:21

# Ahead of the distribution so this layer survives in the build cache across releases,
# instead of being invalidated by every new tarball.
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y vim tzdata && rm -rf /var/lib/apt/lists/*

# Copied straight to its final path so the distribution occupies exactly one published
# layer. Extracting to / and then moving it here would write the whole tree out twice.
# --chown pins ownership to root: without it the files carry the build machine's UID,
# which differs between CI and a local build.
COPY --from=dist --chown=0:0 /dist/benchmark /opt/benchmark

WORKDIR /opt/benchmark

# Start a shell by default
CMD ["/bin/bash"]
