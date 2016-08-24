package elasticsql

import (
	"errors"

	"github.com/xwb1989/sqlparser"
)

// Convert will transform sql to elasticsearch dsl string
func Convert(sql string) (string, error) {
	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		return "", errors.New("parse error")
	}

	//sql valid, start to handle
	resultStr := ""
	switch stmt.(type) {
	case *sqlparser.Select:
		resultStr = handleSelect(stmt.(*sqlparser.Select))
	case *sqlparser.Update:
		return ",", errors.New("update not supported")
	case *sqlparser.Insert:
		return ",", errors.New("insert not supported")
	case *sqlparser.Delete:
		return ",", errors.New("delete not supported")
	}
	return resultStr, nil
}
