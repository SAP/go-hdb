#!/bin/bash
# Run the LDAP authentication test against the compose HANA + OpenLDAP.
# Prerequisite: the LDAP directory has been populated by
#   docker compose exec ldap /ldapConfig.sh
set -euo pipefail

HANA_HOST="hana"
HANA_PORT="39017"

# HANA reports its process up before the SQL port accepts connections; wait for
# the port so the test does not fail with "connection refused".
echo "waiting for HANA SQL port ${HANA_HOST}:${HANA_PORT} ..."
for _ in $(seq 1 60); do
	if (exec 3<>"/dev/tcp/${HANA_HOST}/${HANA_PORT}") 2>/dev/null; then
		break
	fi
	sleep 5
done

cd /src/tests/ldap
echo "running LDAP integration test ..."
export GOHDB_LDAP_HOST="${HANA_HOST}:${HANA_PORT}"
exec go test -count=1 -v --tags ldap ./...
