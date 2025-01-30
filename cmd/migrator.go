package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cernbox/reva-plugins/share/sql"
)

func main() {
	username := flag.String("username", "cernbox_server", "Database username")
	password := flag.String("password", "", "Database password")
	host := flag.String("host", "dbod-cboxeos.cern.ch", "Database host")
	port := flag.Int("port", 5504, "Database port")
	name := flag.String("name", "test", "Database name")
	gatewaysvc := flag.String("gatewaysvc", "localhost:9142", "Gateway service location")
	token := flag.String("token", "", "JWT token for gateway svc")
	dryRun := flag.Bool("dryrun", true, "Use dry run?")

	flag.Parse()

	if *token == "" {
		fmt.Println("Please pass a reva token using `-token`")
		os.Exit(22)
	}

	fmt.Printf("Connecting to %s@%s:%d\n", *username, *host, *port)
	sql.RunMigration(*username, *password, *host, *name, *gatewaysvc, *token, *port, *dryRun)
}
