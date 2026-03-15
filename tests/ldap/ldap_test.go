//go:build ldap

package ldap

import (
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func TestLDAPAuthentication(t *testing.T) {
	const (
		hdbSystemUser     = "SYSTEM"
		hdbSystemPassword = "HanaSystem1234"
		adminDN           = "cn=admin,dc=example,dc=com"
		adminPassword     = "admin1234"
		testUser          = "ldapuser"
		testUserPassword  = "LdapUser1234"
		groupDN           = "cn=hanausers,ou=groups,dc=example,dc=com"
	)

	var (
		hdbHost     = net.JoinHostPort("localhost", "39017")
		ldapHost    = net.JoinHostPort("openldap", "389")
		testUserDN  = fmt.Sprintf("cn=%s,ou=users,dc=example,dc=com", testUser)
		testHDBUser = strings.ToUpper(testUser)
	)

	const createProviderTmpl = `CREATE LDAP PROVIDER LDAP_TEST_PROVIDER
		CREDENTIAL TYPE 'PASSWORD' USING 'user=%s;password=%s'
		USER LOOKUP URL 'ldap://%s/ou=users,dc=example,dc=com??sub?(cn=*)'
		ATTRIBUTE DN 'distinguishedName'
		ATTRIBUTE MEMBER_OF 'memberOf'
		SSL OFF
		DEFAULT ON
		ENABLE PROVIDER`

	exec := func(db *sql.DB, stmt string, fail bool) {
		if _, err := db.Exec(stmt); err != nil && fail {
			t.Fatal(err)
		}
	}

	cleanup := func(db *sql.DB) {
		// Clean up any existing configuration (ignore errors)
		exec(db, fmt.Sprintf("DROP USER %s CASCADE", testHDBUser), false)
		exec(db, "DROP ROLE LDAP_USERS_ROLE", false)
		exec(db, "DROP LDAP PROVIDER LDAP_TEST_PROVIDER", false)
		exec(db, "DROP PSE LDAP_PSE", false)
	}

	configure := func(db *sql.DB) {
		// Create LDAP PSE for encryption (required for LDAP auth)
		exec(db, "CREATE PSE LDAP_PSE", true)
		exec(db, "SET PSE LDAP_PSE PURPOSE LDAP", true)
		// Create LDAP provider - use NetworkAlias since HANA connects via container network
		exec(db, fmt.Sprintf(createProviderTmpl, adminDN, adminPassword, ldapHost), true)
		// Create role mapped to LDAP group
		exec(db, fmt.Sprintf("CREATE ROLE LDAP_USERS_ROLE LDAP GROUP '%s'", groupDN), true)
		// Create HANA user for LDAP authentication
		exec(db, fmt.Sprintf("CREATE USER %s IDENTIFIED EXTERNALLY AS '%s'", testHDBUser, testUserDN), true)
		exec(db, fmt.Sprintf("ALTER USER %s ENABLE LDAP", testHDBUser), true)
		exec(db, fmt.Sprintf("ALTER USER %s AUTHORIZATION LDAP", testHDBUser), true)
	}

	connector := driver.NewBasicAuthConnector(hdbHost, hdbSystemUser, hdbSystemPassword)
	systemDB := sql.OpenDB(connector)
	defer systemDB.Close()

	cleanup(systemDB)
	defer cleanup(systemDB)

	configure(systemDB)

	connector = driver.NewBasicAuthConnector(hdbHost, testHDBUser, testUserPassword)
	db := sql.OpenDB(connector)
	defer db.Close()

	var currentUser string
	if err := db.QueryRow("select current_user from dummy").Scan(&currentUser); err != nil {
		t.Fatal(err)
	}

	if currentUser != testHDBUser {
		t.Fatalf("user %s - expected %s", currentUser, testHDBUser)
	}
}
