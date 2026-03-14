//go:build unit

package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-units"
	"github.com/go-ldap/ldap/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	hanaImage     = "saplabs/hanaexpress:latest"
	hanaMasterPwd = "HanaExpress1"
)

// TestLDAPAuthenticationWithTestcontainers runs the LDAP authentication test
// using testcontainers to manage the infrastructure.
//
// Run with: go test -tags unit -v -timeout 15m ./driver -run TestLDAPAuthenticationWithTestcontainers
func TestLDAPAuthenticationWithTestcontainers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Create a shared network for containers
	testNetwork, err := network.New(ctx, network.WithCheckDuplicate())
	if err != nil {
		t.Fatalf("failed to create network: %v", err)
	}
	defer testNetwork.Remove(ctx) //nolint:errcheck

	// Setup LDAP
	t.Log("Starting OpenLDAP container...")
	ldap, err := setupLDAP(ctx, testNetwork.Name)
	if err != nil {
		t.Fatalf("failed to setup LDAP: %v", err)
	}
	defer ldap.Terminate(ctx)
	t.Logf("OpenLDAP available at %s", ldap.URL)

	// Start HANA Express container
	t.Log("Starting HANA Express container (this may take 5-10 minutes)...")
	hanaContainer, err := startHANAExpress(ctx, testNetwork.Name)
	if err != nil {
		t.Fatalf("failed to start HANA Express: %v", err)
	}
	defer hanaContainer.Terminate(ctx) //nolint:errcheck

	hanaHost, err := hanaContainer.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get HANA host: %v", err)
	}
	hanaPort, err := hanaContainer.MappedPort(ctx, "39017")
	if err != nil {
		t.Fatalf("failed to get HANA port: %v", err)
	}

	t.Logf("HANA Express available at %s:%s", hanaHost, hanaPort.Port())

	// Configure HANA for LDAP authentication
	t.Log("Configuring HANA for LDAP authentication...")
	systemDSN := fmt.Sprintf("hdb://SYSTEM:%s@%s:%s", hanaMasterPwd, hanaHost, hanaPort.Port())
	if err := configureHANAforLDAP(systemDSN, ldap); err != nil {
		t.Fatalf("failed to configure HANA for LDAP: %v", err)
	}

	// Test LDAP authentication
	t.Log("Testing LDAP authentication...")
	ldapUserDSN := fmt.Sprintf("hdb://%s:%s@%s:%s", ldap.TestUser, ldap.TestPassword, hanaHost, hanaPort.Port())
	if err := testLDAPAuth(t, ldapUserDSN, ldap.TestUser); err != nil {
		t.Fatalf("LDAP authentication test failed: %v", err)
	}

	t.Log("LDAP authentication test passed!")
}

// ldapFixture holds LDAP container and connection details.
type ldapFixture struct {
	Container    testcontainers.Container
	URL          string // ldap://host:port for external access
	NetworkAlias string // hostname for containers on same network
	AdminDN      string
	AdminPwd     string
	TestUserDN   string
	TestUser     string
	TestPassword string
	GroupDN      string
}

func (f *ldapFixture) Terminate(ctx context.Context) {
	if f.Container != nil {
		f.Container.Terminate(ctx) //nolint:errcheck
	}
}

func setupLDAP(ctx context.Context, networkName string) (*ldapFixture, error) {
	const (
		image        = "osixia/openldap:latest"
		domain       = "example.com"
		org          = "Test"
		adminPwd     = "admin123"
		testUser     = "ldapuser1"
		testPassword = "LdapPass123"
		networkAlias = "openldap"
	)

	// Start container
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"389/tcp"},
		Networks:     []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {networkAlias},
		},
		Env: map[string]string{
			"LDAP_ORGANISATION":   org,
			"LDAP_DOMAIN":         domain,
			"LDAP_ADMIN_PASSWORD": adminPwd,
			"LDAP_TLS":            "false",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("389/tcp"),
			wait.ForLog("slapd starting"),
		).WithDeadline(2 * time.Minute),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("start container: %w", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		c.Terminate(ctx) //nolint:errcheck
		return nil, fmt.Errorf("get host: %w", err)
	}
	port, err := c.MappedPort(ctx, "389")
	if err != nil {
		c.Terminate(ctx) //nolint:errcheck
		return nil, fmt.Errorf("get port: %w", err)
	}

	f := &ldapFixture{
		Container:    c,
		URL:          fmt.Sprintf("ldap://%s:%s", host, port.Port()),
		NetworkAlias: networkAlias,
		AdminDN:      "cn=admin,dc=example,dc=com",
		AdminPwd:     adminPwd,
		TestUserDN:   fmt.Sprintf("cn=%s,ou=users,dc=example,dc=com", testUser),
		TestUser:     strings.ToUpper(testUser),
		TestPassword: testPassword,
		GroupDN:      "cn=hanausers,ou=groups,dc=example,dc=com",
	}

	// Create users and groups
	if err := f.setupEntries(testUser, testPassword); err != nil {
		c.Terminate(ctx) //nolint:errcheck
		return nil, fmt.Errorf("setup entries: %w", err)
	}

	return f, nil
}

func (f *ldapFixture) setupEntries(testUser, testPassword string) error {
	conn, err := ldap.DialURL(f.URL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	if err := conn.Bind(f.AdminDN, f.AdminPwd); err != nil {
		return fmt.Errorf("bind: %w", err)
	}

	// Helper to add entries idempotently
	add := func(dn string, attrs map[string][]string) error {
		req := ldap.NewAddRequest(dn, nil)
		for k, v := range attrs {
			req.Attribute(k, v)
		}
		if err := conn.Add(req); err != nil && !ldap.IsErrorWithCode(err, ldap.LDAPResultEntryAlreadyExists) {
			return fmt.Errorf("add %s: %w", dn, err)
		}
		return nil
	}

	// Organizational units
	if err := add("ou=users,dc=example,dc=com", map[string][]string{
		"objectClass": {"organizationalUnit"},
		"ou":          {"users"},
	}); err != nil {
		return err
	}

	if err := add("ou=groups,dc=example,dc=com", map[string][]string{
		"objectClass": {"organizationalUnit"},
		"ou":          {"groups"},
	}); err != nil {
		return err
	}

	// Test user
	if err := add(f.TestUserDN, map[string][]string{
		"objectClass":       {"inetOrgPerson", "posixAccount", "extensibleObject"},
		"cn":                {testUser},
		"sn":                {"User1"},
		"uid":               {testUser},
		"uidNumber":         {"10001"},
		"gidNumber":         {"10000"},
		"homeDirectory":     {"/home/" + testUser},
		"userPassword":      {testPassword},
		"distinguishedName": {f.TestUserDN},
	}); err != nil {
		return err
	}

	// Group
	if err := add(f.GroupDN, map[string][]string{
		"objectClass":  {"groupOfUniqueNames"},
		"cn":           {"hanausers"},
		"uniqueMember": {f.TestUserDN},
	}); err != nil {
		return err
	}

	return nil
}

func startHANAExpress(ctx context.Context, networkName string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        hanaImage,
		ExposedPorts: []string{"39017/tcp", "39013/tcp"},
		Networks:     []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"hana"},
		},
		Cmd: []string{
			"--master-password", hanaMasterPwd,
			"--agree-to-sap-license",
		},
		// HANA requires specific resource settings
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.Ulimits = []*units.Ulimit{
				{Name: "nofile", Hard: 1048576, Soft: 1048576},
			}
			// HANA requires specific sysctls - these may need host configuration
			hc.Sysctls = map[string]string{
				"kernel.shmmax":  "1073741824",
				"kernel.shmmni":  "4096",
				"kernel.shmall":  "8388608",
			}
			// HANA requires additional syscalls (move_pages, mbind) - disable seccomp
			hc.SecurityOpt = []string{"seccomp=unconfined"}
		},
		// Wait for HANA to be ready - this takes a while
		WaitingFor: wait.ForLog("Startup finished").WithStartupTimeout(15 * time.Minute),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func configureHANAforLDAP(systemDSN string, ldap *ldapFixture) error {
	connector, err := NewDSNConnector(systemDSN)
	if err != nil {
		return fmt.Errorf("create connector: %w", err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	// Wait for HANA to be fully ready
	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}

	// Clean up any existing configuration (ignore errors)
	db.Exec(`DROP USER ` + ldap.TestUser)
	db.Exec(`DROP ROLE LDAP_USERS_ROLE`)
	db.Exec(`DROP LDAP PROVIDER LDAP_TEST_PROVIDER`)

	// Create LDAP PSE for encryption (required for LDAP auth)
	db.Exec(`CREATE PSE LDAP_PSE`)
	db.Exec(`SET PSE LDAP_PSE PURPOSE LDAP`)

	// Create LDAP provider - use NetworkAlias since HANA connects via container network
	createProvider := fmt.Sprintf(`CREATE LDAP PROVIDER LDAP_TEST_PROVIDER
		CREDENTIAL TYPE 'PASSWORD' USING 'user=%s;password=%s'
		USER LOOKUP URL 'ldap://%s:389/ou=users,dc=example,dc=com??sub?(cn=*)'
		ATTRIBUTE DN 'distinguishedName'
		ATTRIBUTE MEMBER_OF 'memberOf'
		SSL OFF
		DEFAULT ON
		ENABLE PROVIDER`, ldap.AdminDN, ldap.AdminPwd, ldap.NetworkAlias)

	if _, err = db.Exec(createProvider); err != nil {
		return fmt.Errorf("create LDAP provider: %w", err)
	}

	// Create role mapped to LDAP group
	createRole := fmt.Sprintf(`CREATE ROLE LDAP_USERS_ROLE LDAP GROUP '%s'`, ldap.GroupDN)
	if _, err = db.Exec(createRole); err != nil {
		return fmt.Errorf("create LDAP role: %w", err)
	}

	// Create HANA user for LDAP authentication
	createUser := fmt.Sprintf(`CREATE USER %s IDENTIFIED EXTERNALLY AS '%s'`, ldap.TestUser, ldap.TestUserDN)
	if _, err = db.Exec(createUser); err != nil {
		return fmt.Errorf("create LDAP user: %w", err)
	}

	if _, err = db.Exec(`ALTER USER ` + ldap.TestUser + ` ENABLE LDAP`); err != nil {
		return fmt.Errorf("enable LDAP for user: %w", err)
	}

	if _, err = db.Exec(`ALTER USER ` + ldap.TestUser + ` AUTHORIZATION LDAP`); err != nil {
		return fmt.Errorf("set LDAP authorization: %w", err)
	}

	return nil
}

func testLDAPAuth(t *testing.T, dsn, expectedUser string) error {
	connector, err := NewDSNConnector(dsn)
	if err != nil {
		return fmt.Errorf("create connector: %w", err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("LDAP authentication failed: %w", err)
	}

	var currentUser string
	if err := db.QueryRow("SELECT CURRENT_USER FROM DUMMY").Scan(&currentUser); err != nil {
		return fmt.Errorf("query current user: %w", err)
	}

	t.Logf("Successfully authenticated as: %s", currentUser)

	if currentUser != expectedUser {
		return fmt.Errorf("expected user %s, got %s", expectedUser, currentUser)
	}

	return nil
}
