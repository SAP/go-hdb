//go:build x509

package driver

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
)

func TestX509Authentication(t *testing.T) {
	const (
		x509ProviderName   = "X509_TEST_PROVIDER"
		x509ProviderIssuer = "CN=Go-HDB X.509 Tests RootCA"
		x509CertName       = "X509_TEST_CERT"
		x509PSEName        = "X509_TEST_PSE"
	)

	createX509Provider := func(t *testing.T, db *sql.DB, name string, issuer string) {
		if _, err := db.Exec(fmt.Sprintf("CREATE X509 PROVIDER %s WITH ISSUER '%s'", name, issuer)); err != nil {
			t.Fatal(err)
		}
	}

	dropX509Provider := func(t *testing.T, db *sql.DB, name string) {
		if _, err := db.Exec(fmt.Sprintf("DROP X509 PROVIDER %s CASCADE", name)); err != nil {
			t.Fatal(err)
		}
	}

	createCertificate := func(t *testing.T, db *sql.DB, name string, certFilePath string) {
		certPem, err := os.ReadFile(path.Clean(certFilePath))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(fmt.Sprintf("CREATE CERTIFICATE %s FROM '%s'", name, string(certPem))); err != nil {
			t.Fatal(err)
		}
	}

	dropCertificate := func(t *testing.T, db *sql.DB, name string) {
		if _, err := db.Exec(fmt.Sprintf("DROP CERTIFICATE %s", name)); err != nil {
			t.Fatal(err)
		}
	}

	createPSE := func(t *testing.T, db *sql.DB, name string) {
		if _, err := db.Exec(fmt.Sprintf("CREATE PSE %s", name)); err != nil {
			t.Fatal(err)
		}
	}

	dropPSE := func(t *testing.T, db *sql.DB, name string) {
		if _, err := db.Exec(fmt.Sprintf("DROP PSE %s", name)); err != nil {
			t.Fatal(err)
		}
	}

	currentUser := func(t *testing.T, db *sql.DB) string {
		currentUser := ""
		if err := db.QueryRow("select current_user from dummy").Scan(&currentUser); err != nil {
			t.Fatal(err)
		}
		return currentUser
	}

	createUser := func(t *testing.T, db *sql.DB, name, subject string) {
		if _, err := db.Exec(fmt.Sprintf("CREATE USER %s WITH IDENTITY '%s' FOR X509 PROVIDER %s", name, subject, x509ProviderName)); err != nil {
			t.Fatal(err)
		}
	}

	dropUser := func(t *testing.T, db *sql.DB, name string) {
		if _, err := db.Exec(fmt.Sprintf("DROP USER %s CASCADE", name)); err != nil {
			t.Fatal(err)
		}
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

	// open admin connection
	connector := MT.NewConnector()
	db := sql.OpenDB(connector)
	defer db.Close()

	// create the provider
	createX509Provider(t, db, x509ProviderName, x509ProviderIssuer)
	defer dropX509Provider(t, db, x509ProviderName)

	// create the certificate
	testdataDir := MT.TestdataDir(t, "x509")

	createCertificate(t, db, x509CertName, filepath.Join(testdataDir, "rootCA.crt"))
	defer dropCertificate(t, db, x509CertName)

	// create the PSE
	createPSE(t, db, x509PSEName)
	defer dropPSE(t, db, x509PSEName)

	// setup the PSE
	if _, err := db.Exec(fmt.Sprintf("ALTER PSE %s ADD CERTIFICATE %s", x509PSEName, x509CertName)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("SET PSE %s PURPOSE X509 FOR PROVIDER %s", x509PSEName, x509ProviderName)); err != nil {
		t.Fatal(err)
	}

	// test with different key types
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// create the user
			userName := strings.ToUpper(test.name)
			createUser(t, db, userName, test.subject)
			defer dropUser(t, db, userName)

			// test the connection
			clientCertFile := filepath.Join(testdataDir, test.cert)
			clientKeyFile := filepath.Join(testdataDir, test.key)
			userConnector, err := NewX509AuthConnectorByFiles(connector.Host(), clientCertFile, clientKeyFile)
			if err != nil {
				t.Fatal(err)
			}

			userDB := sql.OpenDB(userConnector)
			defer userDB.Close()

			// check the user
			if currentUser(t, userDB) != userName {
				t.Fatalf("Unexpected current user '%s' - '%s' expected", currentUser(t, userDB), userName)
			}
		})
	}
}
