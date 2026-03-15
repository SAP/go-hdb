#!/bin/bash

# Define common variables
LDAP_URL="ldap://localhost:389"
ADMIN_DN="cn=admin,dc=example,dc=com"
ADMIN_PWD="admin1234"
USER_DN="cn=ldapuser,ou=users,dc=example,dc=com"
USER_PWD="LdapUser1234"
GROUP_DN="cn=hanausers,ou=groups,dc=example,dc=com"

# Start by adding the organizational units
cat <<EOF | ldapadd -x -D "$ADMIN_DN" -w "$ADMIN_PWD" -H "$LDAP_URL"
dn: ou=users,dc=example,dc=com
objectClass: organizationalUnit
ou: users

dn: ou=groups,dc=example,dc=com
objectClass: organizationalUnit
ou: groups
EOF

# Add the test user
cat <<EOF | ldapadd -x -D "$ADMIN_DN" -w "$ADMIN_PWD" -H "$LDAP_URL"
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

# Add the group
cat <<EOF | ldapadd -x -D "$ADMIN_DN" -w "$ADMIN_PWD" -H "$LDAP_URL"
dn: $GROUP_DN
objectClass: groupOfUniqueNames
cn: testusers
uniqueMember: $USER_DN
EOF
