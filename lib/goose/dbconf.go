package goose

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/kylelemons/go-gypsy/yaml"
	"github.com/lib/pq"
)

// DBDriver encapsulates the info needed to work with
// a specific database driver
type DBDriver struct {
	Name    string
	OpenStr string
	Import  string
	Dialect SqlDialect
}

type DBConf struct {
	MigrationsDir string
	Env           string
	Driver        DBDriver
	PgSchema      string
}

// extract configuration details from the given file
func NewDBConf(p, env string, pgschema string) (*DBConf, error) {

	cfgFile := filepath.Join(p, "dbconf.yml")

	f, err := yaml.ReadFile(cfgFile)
	if err != nil {
		return nil, err
	}

	drv, err := f.Get(fmt.Sprintf("%s.driver", env))
	if err != nil {
		return nil, err
	}
	drv = os.ExpandEnv(drv)

	open, err := f.Get(fmt.Sprintf("%s.open", env))
	if err != nil {
		return nil, err
	}
	open = os.ExpandEnv(open)

	// Automatically parse postgres urls
	if drv == "postgres" {

		// Assumption: If we can parse the URL, we should
		if parsedURL, err := pq.ParseURL(open); err == nil && parsedURL != "" {
			open = parsedURL
		}
	}

	d := newDBDriver(drv, open)

	// allow the configuration to override the Import for this driver
	if imprt, err := f.Get(fmt.Sprintf("%s.import", env)); err == nil {
		d.Import = imprt
	}

	// allow the configuration to override the Dialect for this driver
	if dialect, err := f.Get(fmt.Sprintf("%s.dialect", env)); err == nil {
		d.Dialect = dialectByName(dialect)
	}

	if !d.IsValid() {
		return nil, errors.New(fmt.Sprintf("Invalid DBConf: %v", d))
	}

	return &DBConf{
		MigrationsDir: filepath.Join(p, "migrations"),
		Env:           env,
		Driver:        d,
		PgSchema:      pgschema,
	}, nil
}

// Create a new DBDriver and populate driver specific
// fields for drivers that we know about.
// Further customization may be done in NewDBConf
func newDBDriver(name, open string) DBDriver {

	d := DBDriver{
		Name:    name,
		OpenStr: open,
	}

	switch name {
	case "postgres":
		d.Import = "github.com/lib/pq"
		d.Dialect = &PostgresDialect{}
	}

	return d
}

// ensure we have enough info about this driver
func (drv *DBDriver) IsValid() bool {
	return len(drv.Import) > 0 && drv.Dialect != nil
}

// OpenDBFromDBConf wraps database/sql.DB.Open() and configures
// the newly opened DB based on the given DBConf.
//
// Callers must Close() the returned DB.
func OpenDBFromDBConf(conf *DBConf) (*sql.DB, error) {
	db, err := sql.Open(conf.Driver.Name, conf.Driver.OpenStr)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if pg_err, ok := err.(*pq.Error); ok {
		if pg_err.Code == "3D000" {
			fmt.Println("Database does not exist. Trying to create it.")
			regex := regexp.MustCompile("dbname=([^ ]+)")
			if m := regex.FindStringSubmatch(conf.Driver.OpenStr); m != nil && len(m) == 2 {
				dbname := m[1]
				masterConnection := regex.ReplaceAllLiteralString(conf.Driver.OpenStr, "dbname=postgres")
				dbm, err := sql.Open(conf.Driver.Name, masterConnection)
				if err != nil {
					return nil, err
				}
				defer dbm.Close()
				if _, err = dbm.Exec(fmt.Sprintf("CREATE DATABASE %s", dbname)); err != nil {
					return nil, err
				}
				//retry to connecto to the now created database
				db, err = sql.Open(conf.Driver.Name, conf.Driver.OpenStr)
			} else {
				return nil, errors.New("Can't create database with unknown name")
			}
		}
	}

	// if a postgres schema has been specified, apply it
	if conf.Driver.Name == "postgres" && conf.PgSchema != "" {
		if _, err := db.Exec("SET search_path TO " + conf.PgSchema); err != nil {
			return nil, err
		}
	}

	return db, nil
}
