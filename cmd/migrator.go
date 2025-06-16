package main

import (
	"flag"
	"fmt"

	"github.com/cernbox/reva-plugins/share/sql"
)

func main() {
	username := flag.String("username", "cernbox_server", "Database username")
	password := flag.String("password", "", "Database password")
	host := flag.String("host", "dbod-cboxeos.cern.ch", "Database host")
	port := flag.Int("port", 5504, "Database port")
	name := flag.String("name", "test", "Database name")
	dryRun := flag.Bool("dryrun", true, "Use dry run?")

	flag.Parse()

	fmt.Printf("Connecting to %s@%s:%d\n", *username, *host, *port)
	sql.Migrate(*username, *password, *host, *name, *port, *dryRun)
}
