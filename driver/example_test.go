package driver_test

import (
	"context"
	"database/sql"
	"log"

	"github.com/SAP/go-hdb/driver"
)

func Example() {
	cfg := driver.NewConnectorConfig()
	cfg.Host, cfg.Username, cfg.Password = "host:port", "user", "password"
	connector, err := driver.NewConfigConnector(cfg)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	if err := db.PingContext(context.Background()); err != nil {
		log.Fatal(err)
	}
}
