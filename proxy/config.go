package proxy

// Config holds proxy connection parameters
type Config struct {
	Address    string
	JWTToken   string
	LocationID string
	User       string
	Password   string
}
