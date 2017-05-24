package elasticsql

import (
	"bytes"

	"encoding/json"

	"github.com/xwb1989/sqlparser"
)

// ConvertPretty will transform sql to elasticsearch dsl, and prettify the output json
func ConvertPretty(sql string) (dsl string, table string, err error) {
	dsl, table, err = Convert(sql)
	if err != nil {
		return dsl, table, err
	}

	var prettifiedDSLBytes bytes.Buffer
	err = json.Indent(&prettifiedDSLBytes, []byte(dsl), "", "  ")
	if err != nil {
		return "", table, err
	}

	return string(prettifiedDSLBytes.Bytes()), table, err
}

// Convert will transform sql to elasticsearch dsl string
func Convert(sql string) (dsl string, table string, err error) {
	stmt, err := sqlparser.Parse(sql)

	if err != nil {
		return "", "", err
	}

	//sql valid, start to handle
	switch stmt.(type) {
	case *sqlparser.Select:
		dsl, table, err = handleSelect(stmt.(*sqlparser.Select))
	case *sqlparser.Update:
		return handleUpdate(stmt.(*sqlparser.Update))
	case *sqlparser.Insert:
		return handleInsert(stmt.(*sqlparser.Insert))
	case *sqlparser.Delete:
		return handleDelete(stmt.(*sqlparser.Delete))
	}

	if err != nil {
		return "", "", err
	}

	return dsl, table, nil
}
