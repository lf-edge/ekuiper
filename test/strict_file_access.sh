#!/bin/bash
# Strict-default file access smoke test.
#
# Runs kuiperd with the production default (allowExternalFileAccess=false)
# and asserts that file paths outside the data directory are denied while
# data-dir relative paths keep working.
#
# Usage (from the repository root, after `make build`):
#   ./test/strict_file_access.sh

set -e

ver=$(git describe --tags --always --match 'v[0-9]*.[0-9]*.[0-9]*' | sed 's/^v//g')
os=$(uname -s | tr "[A-Z]" "[a-z]")
base_dir=_build/kuiper-"$ver"-"$os"-amd64
mkdir -p "$base_dir/data/smoke"
echo '{"a":1}' > "$base_dir/data/smoke/in.lines"
cd "$base_dir"
bin/kuiperd > /tmp/smoke-kuiperd.log 2>&1 &
KUIPER_PID=$!
trap "kill $KUIPER_PID" EXIT
cd - > /dev/null
code=""
for i in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:9081/ || true)
  [ "$code" = "200" ] && break
  sleep 2
done
[ "$code" = "200" ] || { echo "kuiperd did not start"; tail -50 /tmp/smoke-kuiperd.log; exit 1; }
curl -s -X PUT http://127.0.0.1:9081/metadata/sources/file/confKeys/smoke_in \
  -H 'Content-Type: application/json' \
  -d '{"fileType":"lines","path":"data/smoke"}' -o /dev/null
curl -s -X POST http://127.0.0.1:9081/streams \
  -H 'Content-Type: application/json' \
  -d '{"sql":"CREATE STREAM smoke_in () WITH (DATASOURCE=\"in.lines\", FORMAT=\"json\", TYPE=\"file\", CONF_KEY=\"smoke_in\")"}' -o /dev/null
# 1. file sink outside the data dir must be denied with 400
resp=$(curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:9081/rules \
  -H 'Content-Type: application/json' \
  -d '{"id":"smoke_deny_sink","sql":"SELECT * FROM smoke_in","actions":[{"file":{"path":"/tmp/smoke_denied.log","fileType":"lines","format":"json","rollingCount":100}}]}')
code=$(printf "%s" "$resp" | tail -n 1)
body=$(printf "%s" "$resp" | sed '$d')
[ "$code" = "400" ] || { echo "sink outside: unexpected status $code: $body"; exit 1; }
echo "$body" | grep -q "file access denied" || { echo "sink outside not denied: $body"; exit 1; }
# 2. file source outside the data dir must be denied with 400
curl -s -X PUT http://127.0.0.1:9081/metadata/sources/file/confKeys/smoke_etc \
  -H 'Content-Type: application/json' \
  -d '{"fileType":"lines","path":"/etc"}' -o /dev/null
curl -s -X POST http://127.0.0.1:9081/streams \
  -H 'Content-Type: application/json' \
  -d '{"sql":"CREATE STREAM smoke_etc () WITH (DATASOURCE=\"passwd\", FORMAT=\"json\", TYPE=\"file\", CONF_KEY=\"smoke_etc\")"}' -o /dev/null
resp=$(curl -s -w "\n%{http_code}" -X POST http://127.0.0.1:9081/rules \
  -H 'Content-Type: application/json' \
  -d '{"id":"smoke_deny_src","sql":"SELECT * FROM smoke_etc","actions":[{"log":{}}]}')
code=$(printf "%s" "$resp" | tail -n 1)
body=$(printf "%s" "$resp" | sed '$d')
[ "$code" = "400" ] || { echo "source outside: unexpected status $code: $body"; exit 1; }
echo "$body" | grep -q "file access denied" || { echo "source outside not denied: $body"; exit 1; }
# 3. data-dir relative path must keep working
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://127.0.0.1:9081/rules \
  -H 'Content-Type: application/json' \
  -d '{"id":"smoke_allow","sql":"SELECT * FROM smoke_in","actions":[{"file":{"path":"smoke_out.log","fileType":"lines","format":"json","rollingCount":100}}]}')
[ "$code" = "201" ] || { echo "inside path not allowed: $code"; exit 1; }
sleep 3
[ -f "$base_dir/data/smoke_out.log" ] || { echo "smoke_out.log not written"; exit 1; }
for r in smoke_allow smoke_deny_sink smoke_deny_src; do
  curl -s -o /dev/null -X DELETE http://127.0.0.1:9081/rules/$r || true
done
for s in smoke_in smoke_etc; do
  curl -s -o /dev/null -X DELETE http://127.0.0.1:9081/streams/$s || true
done
echo "strict file access smoke passed"
