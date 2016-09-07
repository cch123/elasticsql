package elasticsql

import (
	"errors"

	"github.com/xwb1989/sqlparser"
)

func handleDelete(del *sqlparser.Delete) (string, string, error) {
	return "", "", errors.New("delete not supported")
}
