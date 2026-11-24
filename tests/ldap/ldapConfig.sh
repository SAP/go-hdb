#!/bin/bash

# Define common variables.
# Administer OpenLDAP over the local ldapi:/// socket with SASL EXTERNAL: this
# is the canonical local-admin path (no TCP port, no admin password needed) and
# is independent of whatever TCP port the image listens on.
LDAP_URL="ldapi:///"
USER_DN="cn=ldapuser,ou=users,dc=example,dc=com"
USER_PWD="LdapUser1234"
GROUP_DN="cn=hanausers,ou=groups,dc=example,dc=com"

# Start by adding the organizational units
cat <<EOF | ldapadd -Y EXTERNAL -H "$LDAP_URL"
dn: ou=users,dc=example,dc=com
objectClass: organizationalUnit
ou: users

dn: ou=groups,dc=example,dc=com
objectClass: organizationalUnit
ou: groups
EOF

# Add the test user
cat <<EOF | ldapadd -Y EXTERNAL -H "$LDAP_URL"
dn: $USER_DN
objectClass: inetOrgPerson
objectClass: posixAccount
objectClass: extensibleObject
cn: ldapuser
sn: User1
uid: ldapuser
uidNumber: 10001
gidNumber: 10000
homeDirectory: /home/ldapuser
userPassword: $USER_PWD
distinguishedName: $USER_DN
EOF

# Add the group. groupOfNames/member matches the memberof overlay config
# (OPENLDAP_BOOTSTRAP_MEMBEROF), so members get a memberOf attribute that HANA
# uses to map the LDAP group to a role.
cat <<EOF | ldapadd -Y EXTERNAL -H "$LDAP_URL"
dn: $GROUP_DN
objectClass: groupOfNames
cn: testusers
member: $USER_DN
EOF
