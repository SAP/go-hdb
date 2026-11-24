#!/bin/bash
# Install the Kerberos keytab into the HANA instance and restart it so the
# server reads it (SAP Note 1837331). Run inside the HANA container, which
# executes as <sid>adm (hxeadm):
#
#   docker compose exec hana bash /hanaKrbSetup.sh
#
set -euo pipefail

HOME_ETC="${HOME}/etc"            # /usr/sap/HXE/home/etc, owned by hxeadm
REALM="EXAMPLE.TEST"

mkdir -p "${HOME_ETC}"

# the HANA realm configuration
cat >"${HOME_ETC}/krb5_hdb.conf" <<EOF
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_kdc = false
    dns_lookup_realm = false

[realms]
    ${REALM} = {
        kdc = kdc
        admin_server = kdc
    }

[domain_realm]
    .example.test = ${REALM}
    example.test = ${REALM}
EOF

cp /keytab/krb5_hdb.keytab "${HOME_ETC}/krb5_hdb.keytab"
chmod 600 "${HOME_ETC}/krb5_hdb.conf" "${HOME_ETC}/krb5_hdb.keytab"

# restart HANA so it picks up the keytab/config (login shell for the HDB PATH)
bash -lc "HDB stop"
bash -lc "HDB start"
echo "HANA-KRB-SETUP-DONE"

