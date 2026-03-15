//go:build x509

package x509

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/SAP/go-hdb/driver"
)

func TestX509Authentication(t *testing.T) {
	const (
		x509ProviderName   = "X509_TEST_PROVIDER"
		x509ProviderIssuer = "CN=Go-HDB X.509 Tests RootCA"
		x509CertName       = "X509_TEST_CERT"
		x509PSEName        = "X509_TEST_PSE"
	)

	exec := func(db *sql.DB, stmt string, fail bool) {
		if _, err := db.Exec(stmt); err != nil && fail {
			t.Fatal(err)
		}
	}

	cleanup := func(db *sql.DB) {
		exec(db, fmt.Sprintf("DROP PSE %s", x509PSEName), false)
		exec(db, fmt.Sprintf("DROP CERTIFICATE %s", x509CertName), false)
		exec(db, fmt.Sprintf("DROP X509 PROVIDER %s CASCADE", x509ProviderName), false)
	}

	configure := func(db *sql.DB, root *os.Root) {
		exec(db, fmt.Sprintf("CREATE X509 PROVIDER %s WITH ISSUER '%s'", x509ProviderName, x509ProviderIssuer), true)

		certPem, err := root.ReadFile("rootCA.crt")
		if err != nil {
			t.Fatal(err)
		}
		exec(db, fmt.Sprintf("CREATE CERTIFICATE %s FROM '%s'", x509CertName, string(certPem)), true)
		exec(db, fmt.Sprintf("CREATE PSE %s", x509PSEName), true)
		exec(db, fmt.Sprintf("ALTER PSE %s ADD CERTIFICATE %s", x509PSEName, x509CertName), true)
		exec(db, fmt.Sprintf("SET PSE %s PURPOSE X509 FOR PROVIDER %s", x509PSEName, x509ProviderName), true)
	}

	createUser := func(db *sql.DB, name, subject string) {
		exec(db, fmt.Sprintf("CREATE USER %s WITH IDENTITY '%s' FOR X509 PROVIDER %s", name, subject, x509ProviderName), true)
	}

	dropUser := func(db *sql.DB, name string) {
		exec(db, fmt.Sprintf("DROP USER %s CASCADE", name), false)
	}

	tests := []struct {
		name    string
		subject string
		key     string
		cert    string
	}{
		{"testuser_rsa_pkcs1", "CN=GoHDBTestUser_rsa", "rsa.pkcs1.key", "rsa.crt"},
		{"testuser_rsa_pkcs8", "CN=GoHDBTestUser_rsa", "rsa.pkcs8.key", "rsa.crt"},
		{"testuser_ec_p256", "CN=GoHDBTestUser_ec_p256", "ec_p256.ec.key", "ec_p256.crt"},
		{"testuser_ec_p256_pkcs8", "CN=GoHDBTestUser_ec_p256", "ec_p256.pkcs8.key", "ec_p256.crt"},
		{"testuser_ec_p384", "CN=GoHDBTestUser_ec_p384", "ec_p384.ec.key", "ec_p384.crt"},
		{"testuser_ec_p384_pkcs8", "CN=GoHDBTestUser_ec_p384", "ec_p384.pkcs8.key", "ec_p384.crt"},
		{"testuser_ec_p521", "CN=GoHDBTestUser_ec_p521", "ec_p521.ec.key", "ec_p521.crt"},
		{"testuser_ec_p521_pkcs8", "CN=GoHDBTestUser_ec_p521", "ec_p521.pkcs8.key", "ec_p521.crt"},
		{"testuser_ed25519", "CN=GoHDBTestUser_ed25519", "ed25519.key", "ed25519.crt"},
	}

	const envDSN = "GOHDBDSN"

	dsnStr, ok := os.LookupEnv(envDSN)
	if !ok {
		t.Fatalf("environment variable %s not set", envDSN)
	}

	var err error
	connector, err := driver.NewDSNConnector(dsnStr)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)

	cleanup(db)

	t.Cleanup(func() {
		cleanup(db)
		db.Close()
	})

	testdataDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(testdataDir)
	if err != nil {
		t.Fatal(err)
	}

	configure(db, root)

	// test with different key types
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// create the user
			userName := strings.ToUpper(test.name)
			dropUser(db, userName)
			defer dropUser(db, userName)
			createUser(db, userName, test.subject)

			// test the connection
			clientCert, err := root.ReadFile(test.cert)
			if err != nil {
				t.Fatal(err)
			}
			clientKey, err := root.ReadFile(test.key)
			if err != nil {
				t.Fatal(err)
			}
			userConnector, err := driver.NewX509AuthConnector(connector.Host(), clientCert, clientKey)
			if err != nil {
				t.Fatal(err)
			}

			userDB := sql.OpenDB(userConnector)
			defer userDB.Close()

			// check the user
			var currentUser string
			if err := userDB.QueryRow("select current_user from dummy").Scan(&currentUser); err != nil {
				t.Fatal(err)
			}
			if currentUser != userName {
				t.Fatalf("Unexpected current user '%s' - '%s' expected", currentUser, userName)
			}
		})
	}
}
