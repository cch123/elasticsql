package elasticsql

import (
	"errors"

	"github.com/xwb1989/sqlparser"
)

// Convert will transform sql to elasticsearch dsl string
func Convert(sql string) (dsl string, table string, err error) {
	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		return "", "", errors.New("parse error")
	}

	//sql valid, start to handle
	switch stmt.(type) {
	case *sqlparser.Select:
		dsl, table, err = handleSelect(stmt.(*sqlparser.Select))
	case *sqlparser.Update:
		return "", "", errors.New("update not supported")
	case *sqlparser.Insert:
		return "", "", errors.New("insert not supported")
	case *sqlparser.Delete:
		return "", "", errors.New("delete not supported")
	}

	if err != nil {
		return "", "", err
	}

	return dsl, table, nil
}
