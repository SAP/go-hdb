//go:build !unit

package driver

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func testExistSessionVariables(sv1, sv2 map[string]string, t *testing.T) {
	for k1, v1 := range sv1 {
		v2, ok := sv2[k1]
		if !ok {
			t.Fatalf("session variable value for %s does not exist", k1)
		}
		if v2 != v1 {
			t.Fatalf("session variable value for %s is %s - expected %s", k1, v2, v1)
		}
	}
}

func testNotExistSessionVariables(keys []string, sv2 map[string]string, t *testing.T) {
	for _, k1 := range keys {
		v2, ok := sv2[k1]
		if ok && v2 != "" {
			t.Fatalf("session variable value for %s is %s - should be empty", k1, v2)
		}
	}
}

func testSessionVariables(t *testing.T) {
	connector := NewTestConnector()

	// set session variables
	sv1 := SessionVariables{"k1": "v1", "k2": "v2", "k3": "v3"}
	connector.SetSessionVariables(sv1)

	// check session variables
	db := sql.OpenDB(connector)
	defer db.Close()

	// retrieve session variables
	sv2, err := querySessionVariables(db)
	if err != nil {
		t.Fatal(err)
	}

	// check if session variables are set after connect to db.
	testExistSessionVariables(sv1, sv2, t)
	testNotExistSessionVariables([]string{"k4"}, sv2, t)
}

func printInvalidConnectAttempts(t *testing.T, username string) {
	db := DefaultTestDB()

	if invalidConnectAttempts, err := queryInvalidConnectAttempts(db, username); err != nil {
		t.Logf("error in selecting invalid connect attempts: %s", err)
	} else {
		t.Logf("number of invalid connect attempts: %d", invalidConnectAttempts)
	}
}

func testRetryConnect(t *testing.T) {
	const invalidPassword = "invalid_password"

	connector := NewTestConnector()

	password := connector.Password() // safe password
	refreshPassword := func() (string, bool) {
		printInvalidConnectAttempts(t, connector.Username())
		return password, true
	}
	connector.SetPassword(invalidPassword) // set invalid password
	connector.SetRefreshPassword(refreshPassword)
	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestConnector(t *testing.T) {
	tests := []struct {
		name string
		fct  func(t *testing.T)
	}{
		{"testSessionVariables", testSessionVariables},
		{"testRetryConnect", testRetryConnect},
	}

	for i := range tests {
		func(i int) {
			t.Run(tests[i].name, func(t *testing.T) {
				t.Parallel()
				tests[i].fct(t)
			})
		}(i)
	}
}

const X509_POVIDER_NAME = "X509_TEST_PROVIDER"
const X509_POVIDER_ISSUER = "CN=Go-HDB X.509 Tests RootCA"
const X509_CERT_NAME = "X509_TEST_CERT"
const X509_PSE_NAME = "X509_TEST_PSE"

func TestX509Authentication(t *testing.T) {
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
	connector := NewTestConnector()
	db := sql.OpenDB(connector)
	defer db.Close()

	// create the provider
	if err := createX509Provider(db, X509_POVIDER_NAME, X509_POVIDER_ISSUER); err != nil {
		t.Fatal(err)
	}
	defer dropX509Provider(db, X509_POVIDER_NAME)

	// create the certificate
	certFolder := certFolder()
	if err := createCertificateByFile(db, X509_CERT_NAME, filepath.Join(certFolder, "rootCA.crt")); err != nil {
		t.Fatal(err)
	}
	defer dropCertificate(db, X509_CERT_NAME)

	// create the PSE
	if err := createPSE(db, X509_PSE_NAME); err != nil {
		t.Fatal(err)
	}
	defer dropPSE(db, X509_PSE_NAME)

	// setup the PSE
	if _, err := db.Exec(fmt.Sprintf("ALTER PSE %s ADD CERTIFICATE %s", X509_PSE_NAME, X509_CERT_NAME)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("SET PSE %s PURPOSE X509 FOR PROVIDER %s", X509_PSE_NAME, X509_POVIDER_NAME)); err != nil {
		t.Fatal(err)
	}

	// test with different key types
	for i := range tests {
		func(i int) {
			t.Run(tests[i].name, func(t *testing.T) {
				// create the user
				userName := strings.ToUpper(tests[i].name)
				subject := tests[i].subject
				if _, err := db.Exec(fmt.Sprintf("CREATE USER %s WITH IDENTITY '%s' FOR X509 PROVIDER %s", userName, subject, X509_POVIDER_NAME)); err != nil {
					t.Fatal(err)
				}
				defer dropUser(db, userName)
				log.Printf("created user %s", userName)

				// test the connection
				clientCertFile := filepath.Join(certFolder, tests[i].cert)
				clientKeyFile := filepath.Join(certFolder, tests[i].key)
				userConnector, err := NewX509AuthConnectorByFiles(connector.Host(), clientCertFile, clientKeyFile)
				if err != nil {
					t.Fatal(err)
				}

				userDb := sql.OpenDB(userConnector)
				defer userDb.Close()

				// check the user
				currentUser, err := queryCurrentUser(userDb)
				if err != nil {
					t.Fatal(err)
				}
				if currentUser != userName {
					t.Fatalf("Unexpected current user '%s' - '%s' expected", currentUser, userName)
				}
			})
		}(i)
	}
}

func certFolder() string {
	_, b, _, _ := runtime.Caller(0)

	// Root folder of this project
	return filepath.Join(filepath.Dir(b), "../testdata/x509")
}

func createX509Provider(db *sql.DB, name string, issuer string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE X509 PROVIDER %s WITH ISSUER '%s'", name, issuer))
	if err == nil {
		log.Printf("created X.509 provider %s", name)
	}
	return err
}

func dropX509Provider(db *sql.DB, name string) {
	_, err := db.Exec(fmt.Sprintf("DROP X509 PROVIDER %s CASCADE", name))
	if err == nil {
		log.Printf("dropped X.509 provider %s", name)
	} else {
		log.Printf("failed to drop X.509 provider %s: %v: ", name, err)
	}
}

func createCertificate(db *sql.DB, name string, pem string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE CERTIFICATE %s FROM '%s'", name, pem))
	if err == nil {
		log.Printf("created certificate %s", name)
	}
	return err
}

func createCertificateByFile(db *sql.DB, name string, certFilePath string) error {
	certPem, err := os.ReadFile(path.Clean(certFilePath))
	if err != nil {
		return err
	}
	return createCertificate(db, name, string(certPem))
}

func dropCertificate(db *sql.DB, name string) {
	_, err := db.Exec(fmt.Sprintf("DROP CERTIFICATE %s", name))
	if err == nil {
		log.Printf("dropped certificate %s", name)
	} else {
		log.Printf("failed to drop certificate %s: %v: ", name, err)
	}
}

func createPSE(db *sql.DB, name string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE PSE %s", name))
	if err == nil {
		log.Printf("created PSE %s", name)
	}
	return err
}

func dropPSE(db *sql.DB, name string) {
	_, err := db.Exec(fmt.Sprintf("DROP PSE %s", name))
	if err == nil {
		log.Printf("dropped PSE %s", name)
	} else {
		log.Printf("failed to drop PSE %s: %v: ", name, err)
	}
}

func dropUser(db *sql.DB, name string) {
	_, err := db.Exec(fmt.Sprintf("DROP USER %s CASCADE", name))
	if err == nil {
		log.Printf("dropped user %s", name)
	} else {
		log.Printf("failed to drop user %s: %v: ", name, err)
	}
}
