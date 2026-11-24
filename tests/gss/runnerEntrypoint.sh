#!/bin/bash
# Obtain a Kerberos ticket for the test principal, then run the GSS test.
set -euo pipefail

REALM="EXAMPLE.TEST"
PRINCIPAL="gssuser@${REALM}"
PASSWORD="GssUser1234"
HANA_HOST="hana"
HANA_PORT="39017"

echo "obtaining TGT for ${PRINCIPAL} ..."
echo "${PASSWORD}" | kinit "${PRINCIPAL}"
klist

# HANA reports its process up before the SQL port accepts connections; wait for
# the port so the test does not fail with "connection refused".
echo "waiting for HANA SQL port ${HANA_HOST}:${HANA_PORT} ..."
for _ in $(seq 1 60); do
	if (exec 3<>"/dev/tcp/${HANA_HOST}/${HANA_PORT}") 2>/dev/null; then
		break
	fi
	sleep 5
done

cd /src/tests/gss
echo "running GSS integration test ..."
export GOHDB_GSS_HOST="${HANA_HOST}:${HANA_PORT}"
exec go test -count=1 -v --tags "gss gssapi" ./...
