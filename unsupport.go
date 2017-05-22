package elasticsql

import (
	"errors"

	"github.com/xwb1989/sqlparser"
)

func handleUpdate(upd *sqlparser.Update) (string, string, error) {
	return "", "", errors.New("update not supported")
}

func handleDelete(del *sqlparser.Delete) (string, string, error) {
	return "", "", errors.New("delete not supported")
}

func handleInsert(ins *sqlparser.Insert) (string, string, error) {
	return "", "", errors.New("insert not supported")
}
