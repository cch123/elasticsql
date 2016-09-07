package elasticsql

import (
	"errors"

	"github.com/xwb1989/sqlparser"
)

func handleUpdate(upd *sqlparser.Update) (string, string, error) {
	return "", "", errors.New("update not supported")
}
