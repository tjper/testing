package testing

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

// SelectJSON does a SQL SELECT and returns the result at JSON. This makes
// SELECT results easily comparable.
func SelectJSON(db sqlx.Queryer, sql string, args ...interface{}) ([]byte, error) {
	sql = `WITH t AS (
    ` + sql + ` 
  ) SELECT json_agg(t) FROM t`

	var j []byte
	if err := db.QueryRowx(sql, args...).Scan(&j); err != nil {
		return nil, errors.Wrapf(err, "failed to SelectAsJSON\nsql = %s\nargs = %v", sql, args)
	}
	return j, nil

}

// DisableTriggers enables triggers on the tables specified. This is typically
// used to disable foreign key constraints on a table for testing.
//
// WARNING: When using this function ensure your db connection is pointed at a
//          localhost database.
func DisableTriggers(db sqlx.Execer, tables ...string) error {
	SQL := `
    ALTER TABLE %s DISABLE TRIGGER ALL
  `
	for _, table := range tables {
		if _, err := db.Exec(fmt.Sprintf(SQL, table)); err != nil {
			return err
		}
	}
	return nil
}

// EnableTriggers enables triggers on the tables specified. This is typically
// used to re-enable foreign key constraints on a table after testing.
//
// WARNING: When using this function ensure your db connection is pointed at a
//          localhost database.
func EnableTriggers(db sqlx.Execer, tables ...string) error {
	SQL := `
    ALTER TABLE %s ENABLE TRIGGER ALL
  `
	for _, table := range tables {
		if _, err := db.Exec(fmt.Sprintf(SQL, table)); err != nil {
			return err
		}
	}
	return nil
}

// TruncateTable truncates all data from the tables specified.
//
// WARNING: When using this function ensure your db connection is pointed at a
//          localhost database.
func TruncateTable(db sqlx.Execer, tables ...string) error {
	SQL := `
    TRUNCATE TABLE %s CASCADE
  `
	for _, table := range tables {
		if _, err := db.Exec(fmt.Sprintf(SQL, table)); err != nil {
			return err
		}
	}
	return nil
}

// CopyFrom performs a psql COPY FROM statement using the table and data
// specified as arguments. COPY FROM inserts json data into the table.
//
// WARNING: When using this function ensure your db connection is pointed at a
//          localhost database.
func CopyFrom(db sqlx.Preparer, table string, data []map[string]interface{}) error {
	errMsg := fmt.Sprintf("failed to run CopyFrom\ntable = %s\ndata = %v\n", table, data)

	columns := make([]string, 0)
	for column := range data[0] {
		columns = append(columns, column)
	}

	stmt, err := db.Prepare(pq.CopyIn(table, columns...))
	if err != nil {
		return errors.Wrap(err, errMsg)
	}

	for _, m := range data {
		record, err := jsonMapToSlice(columns, m)
		if err != nil {
			return errors.Wrap(err, errMsg)
		}

		if _, err := stmt.Exec(record...); err != nil {
			return errors.Wrap(err, errMsg)
		}
	}

	if _, err := stmt.Exec(); err != nil {
		return errors.Wrap(err, errMsg)
	}
	if err := stmt.Close(); err != nil {
		return errors.Wrap(err, errMsg)
	}
	return nil
}

func jsonMapToSlice(columns []string, jsonMap map[string]interface{}) ([]interface{}, error) {
	var (
		slice = make([]interface{}, len(jsonMap))
		ok    bool
	)
	for i, column := range columns {
		slice[i], ok = jsonMap[column]
		if !ok {
			return nil, errors.Errorf("json field \"%s\" does not exist", column)
		}
	}
	return slice, nil
}
