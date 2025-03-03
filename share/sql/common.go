package sql

import (
	"fmt"

	"github.com/cs3org/reva"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const (
	projectInstancesPrefix        = "newproject"
	projectSpaceGroupsPrefix      = "cernbox-project-"
	projectSpaceAdminGroupsSuffix = "-admins"
	projectPathPrefix             = "/eos/project/"
)

type config struct {
	Engine               string `mapstructure:"engine"` // mysql | sqlite
	DBUsername           string `mapstructure:"db_username"`
	DBPassword           string `mapstructure:"db_password"`
	DBHost               string `mapstructure:"db_host"`
	DBPort               int    `mapstructure:"db_port"`
	DBName               string `mapstructure:"db_name"`
	GatewaySvc           string `mapstructure:"gatewaysvc"`
	LinkPasswordHashCost int    `mapstructure:"password_hash_cost"`
}

func init() {
	reva.RegisterPlugin(shareMgr{})
	reva.RegisterPlugin(publicShareMgr{})
}

func getDb(c config) (*gorm.DB, error) {
	switch c.Engine {
	case "sqlite":
		return gorm.Open(sqlite.Open(c.DBName), &gorm.Config{})
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", c.DBUsername, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
		return gorm.Open(mysql.Open(dsn), &gorm.Config{})
	default: // default is mysql
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", c.DBUsername, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
		return gorm.Open(mysql.Open(dsn), &gorm.Config{})
	}
}
