//go:build gss && gssapi && linux

package gss

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/SAP/go-hdb/driver"
	"github.com/SAP/go-hdb/driver/gss"
)

// TestGSSAuthentication authenticates to HANA with Kerberos via GSS-API.
//
// It requires the docker-compose rig (MIT krb5 KDC + HANA Express) brought up
// and configured via gssConfig.sh, and a valid TGT for the test principal in
// the credential cache (kinit). See README.md. Build with -tags "gss gssapi".
func TestGSSAuthentication(t *testing.T) {
	const (
		hdbSystemUser     = "SYSTEM"
		hdbSystemPassword = "HanaSystem1234"
		realm             = "EXAMPLE.TEST"
		testPrincipal     = "gssuser@" + realm
		testHDBUser       = "GSSUSER"
	)

	// HANA host:port, set by the runner container to the network alias
	// (GOHDB_GSS_HOST=hana:39017).
	hdbHost := os.Getenv("GOHDB_GSS_HOST")
	if hdbHost == "" {
		t.Fatal("GOHDB_GSS_HOST not set")
	}

	exec := func(db *sql.DB, stmt string, fail bool) {
		if _, err := db.Exec(stmt); err != nil && fail {
			t.Fatal(err)
		}
	}

	cleanup := func(db *sql.DB) {
		exec(db, "DROP USER "+testHDBUser+" CASCADE", false)
	}

	configure := func(db *sql.DB) {
		// map the Kerberos principal to a HANA user and enable it for Kerberos
		// (SAP Note 1837331).
		exec(db, "CREATE USER "+testHDBUser+" WITH IDENTITY '"+testPrincipal+"' FOR KERBEROS", true)
	}

	// SYSTEM connects with password to configure the Kerberos user.
	sysCfg := driver.NewConnectorConfig()
	sysCfg.Host = hdbHost
	sysCfg.Username = hdbSystemUser
	sysCfg.Password = hdbSystemPassword
	sysConnector, err := driver.NewConfigConnector(sysCfg)
	if err != nil {
		t.Fatal(err)
	}
	systemDB := sql.OpenDB(sysConnector)
	defer systemDB.Close()

	cleanup(systemDB)
	defer cleanup(systemDB)
	configure(systemDB)

	// the test principal connects via GSS: no username/password, only the
	// Kerberos initiator. The server maps the principal to testHDBUser.
	gssCfg := driver.NewConnectorConfig()
	gssCfg.Host = hdbHost
	gssCfg.AuthMethods = driver.AuthGSS
	gssCfg.GSS = gss.NewInitiator
	gssConnector, err := driver.NewConfigConnector(gssCfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(gssConnector)
	defer db.Close()

	var currentUser string
	if err := db.QueryRow("select current_user from dummy").Scan(&currentUser); err != nil {
		t.Fatal(err)
	}
	if strings.ToUpper(currentUser) != testHDBUser {
		t.Fatalf("user %s - expected %s", currentUser, testHDBUser)
	}
}
