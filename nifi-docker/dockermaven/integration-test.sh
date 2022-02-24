#!/bin/bash

#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

set -exuo pipefail

TAG=$1
VERSION=$2


trap '{ docker ps -qaf Name="nifi-${TAG}-integration-test" | xargs --no-run-if-empty docker rm -f; }' EXIT

echo "Deleting any existing nifi-${TAG}-integration-test containers"
docker ps -qaf Name="nifi-${TAG}-integration-test" | xargs --no-run-if-empty docker rm -f;

echo "Checking that all files are owned by NiFi"
test -z "$(docker run --rm --entrypoint /bin/bash "thaihv/setl-nifi:${TAG}" -c "find /opt/nifi ! -user nifi")"

echo "Checking environment variables"
test "/opt/nifi/nifi-current" = "$(docker run --rm --entrypoint /bin/bash "thaihv/setl-nifi:${TAG}" -c 'echo -n $NIFI_HOME')"
test "/opt/nifi/nifi-current" = "$(docker run --rm --entrypoint /bin/bash "thaihv/setl-nifi:${TAG}" -c "readlink \${NIFI_BASE_DIR}/nifi-${VERSION}")"
test "/opt/nifi/nifi-toolkit-current" = "$(docker run --rm --entrypoint /bin/bash "thaihv/setl-nifi:${TAG}" -c "readlink \${NIFI_BASE_DIR}/nifi-toolkit-${VERSION}")"

test "/opt/nifi/nifi-current/logs" = "$(docker run --rm --entrypoint /bin/bash "thaihv/setl-nifi:${TAG}" -c 'echo -n $NIFI_LOG_DIR')"
test "/opt/nifi/nifi-current/run" = "$(docker run --rm --entrypoint /bin/bash "thaihv/setl-nifi:${TAG}" -c 'echo -n $NIFI_PID_DIR')"
test "/opt/nifi" = "$(docker run --rm --entrypoint /bin/bash "thaihv/setl-nifi:${TAG}" -c 'echo -n $NIFI_BASE_DIR')"

echo "Starting NiFi container..."
docker run -d --name "nifi-${TAG}-integration-test" "thaihv/setl-nifi:${TAG}"

IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "nifi-${TAG}-integration-test")

for i in $(seq 1 10) :; do
    echo "Iteration: ${i}"
    if docker exec "nifi-${TAG}-integration-test" bash -c " echo Running < /dev/tcp/${IP}/8443"; then
        break
    fi
    sleep 10
done

echo "Checking NiFi REST API Access"
test "200" = "$(docker exec "nifi-${TAG}-integration-test" bash -c "curl -s -o /dev/null -w %{http_code} -k https://${IP}:8443/nifi-api/access")"

echo "Stopping NiFi container"
time docker stop "nifi-${TAG}-integration-test"
