#!/bin/bash
# Set up an MIT Kerberos KDC for the GSS integration test and keep it running.
#
# Realm:   EXAMPLE.TEST
# Client:  gssuser@EXAMPLE.TEST           (password: GssUser1234)
# Service: hdb/hana@EXAMPLE.TEST          (keytab exported to /keytab/hana.keytab)
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
REALM="EXAMPLE.TEST"
KADMIN_PASS="kadmin1234"
CLIENT_PRINCIPAL="gssuser@${REALM}"
CLIENT_PASSWORD="GssUser1234"
SERVICE_PRINCIPAL="hdb/hana@${REALM}"

apt-get update
apt-get install -y krb5-kdc krb5-admin-server krb5-user

# krb5.conf
cat >/etc/krb5.conf <<EOF
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

# KDC database (stash file, no master-password prompt)
kdb5_util create -s -r "${REALM}" -P "${KADMIN_PASS}"

# principals
kadmin.local -q "addprinc -pw ${CLIENT_PASSWORD} ${CLIENT_PRINCIPAL}"
kadmin.local -q "addprinc -randkey ${SERVICE_PRINCIPAL}"

# export the service keytab for HANA onto the shared volume. HANA reads it as
# krb5_hdb.keytab from the <sid>adm $HOME/etc directory (SAP Note 1837331).
rm -f /keytab/krb5_hdb.keytab
kadmin.local -q "ktadd -k /keytab/krb5_hdb.keytab ${SERVICE_PRINCIPAL}"
chmod 644 /keytab/krb5_hdb.keytab

# run the KDC in the foreground so the container stays up
krb5kdc -n
