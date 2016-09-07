package elasticsql

import (
	"errors"

	"github.com/xwb1989/sqlparser"
)

func handleInsert(ins *sqlparser.Insert) (string, string, error) {
	return "", "", errors.New("insert not supported")
}
