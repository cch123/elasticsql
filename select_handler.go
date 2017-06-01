package elasticsql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func handleSelect(sel *sqlparser.Select) (dsl string, esType string, err error) {

	// Handle where
	// top level node pass in an empty interface
	// to tell the children this is root
	// is there any better way?
	var rootParent sqlparser.BoolExpr
	var defaultQueryMapStr = `{"bool" : {"must": [{"match_all" : {}}]}}`
	var queryMapStr string

	// use may not pass where clauses
	if sel.Where != nil {
		queryMapStr, err = handleSelectWhere(&sel.Where.Expr, true, &rootParent)
		if err != nil {
			return "", "", err
		}
	}
	if queryMapStr == "" {
		queryMapStr = defaultQueryMapStr
	}

	//TODO support multiple tables
	//for i, fromExpr := range sel.From {
	//	fmt.Printf("the %d of from is %#v\n", i, sqlparser.String(fromExpr))
	//}

	//Handle from
	if len(sel.From) != 1 {
		return "", "", errors.New("elasticsql: multiple from currently not supported")
	}
	esType = sqlparser.String(sel.From)
	esType = strings.Replace(esType, "`", "", -1)

	queryFrom, querySize := "0", "1"

	aggFlag := false
	// if the request is to aggregation
	// then set aggFlag to true, and querySize to 0
	// to not return any query result

	var aggStr string
	if len(sel.GroupBy) > 0 {
		aggFlag = true
		querySize = "0"
		aggStr, err = buildAggs(sel)
		if err != nil {
			//aggStr = ""
			return "", "", err
		}
	}

	// Handle limit
	if sel.Limit != nil {
		if sel.Limit.Offset != nil {
			queryFrom = sqlparser.String(sel.Limit.Offset)
		}
		querySize = sqlparser.String(sel.Limit.Rowcount)
	}

	// Handle order by
	// when executating aggregations, order by is useless
	var orderByArr []string
	if aggFlag == false {
		for _, orderByExpr := range sel.OrderBy {
			orderByStr := fmt.Sprintf(`{"%v": "%v"}`, sqlparser.String(orderByExpr.Expr), orderByExpr.Direction)
			orderByArr = append(orderByArr, orderByStr)
		}
	}

	resultMap := make(map[string]interface{})
	resultMap["query"] = queryMapStr
	resultMap["from"] = queryFrom
	resultMap["size"] = querySize
	if len(aggStr) > 0 {
		resultMap["aggregations"] = aggStr
	}

	if len(orderByArr) > 0 {
		resultMap["sort"] = fmt.Sprintf("[%v]", strings.Join(orderByArr, ","))
	}

	// keep the travesal in order, avoid unpredicted json
	var keySlice = []string{"query", "from", "size", "sort", "aggregations"}
	var resultArr []string
	for _, mapKey := range keySlice {
		if val, ok := resultMap[mapKey]; ok {
			resultArr = append(resultArr, fmt.Sprintf(`"%v" : %v`, mapKey, val))
		}
	}

	dsl = "{" + strings.Join(resultArr, ",") + "}"
	return dsl, esType, nil
}

func buildNestedFuncStrValue(nestedFunc *sqlparser.FuncExpr) (string, error) {
	var result string
	switch string(nestedFunc.Name) {
	case "group_concat":
		for _, nestedExpr := range nestedFunc.Exprs {
			switch nestedExpr.(type) {
			case *sqlparser.NonStarExpr:
				nonStarExpr := nestedExpr.(*sqlparser.NonStarExpr)
				result += strings.Trim(sqlparser.String(nonStarExpr), `'`)
			default:
				return "", errors.New("elasticsql: unsupported expression" + sqlparser.String(nestedExpr))
			}
		}
		//TODO support more functions
	default:
		return "", errors.New("elasticsql: unsupported function" + string(nestedFunc.Name))
	}
	return result, nil
}

func handleSelectWhereAndExpr(expr *sqlparser.BoolExpr, topLevel bool, parent *sqlparser.BoolExpr) (string, error) {
	andExpr := (*expr).(*sqlparser.AndExpr)
	leftExpr := andExpr.Left
	rightExpr := andExpr.Right
	leftStr, err := handleSelectWhere(&leftExpr, false, expr)
	if err != nil {
		return "", err
	}
	rightStr, err := handleSelectWhere(&rightExpr, false, expr)
	if err != nil {
		return "", err
	}

	// not toplevel
	// if the parent node is also and, then the result can be merged

	var resultStr string
	if leftStr == "" || rightStr == "" {
		resultStr = leftStr + rightStr
	} else {
		resultStr = leftStr + `,` + rightStr
	}

	if _, ok := (*parent).(*sqlparser.AndExpr); ok {
		return resultStr, nil
	}
	return fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr), nil
}

func handleSelectWhereOrExpr(expr *sqlparser.BoolExpr, topLevel bool, parent *sqlparser.BoolExpr) (string, error) {
	orExpr := (*expr).(*sqlparser.OrExpr)
	leftExpr := orExpr.Left
	rightExpr := orExpr.Right

	leftStr, err := handleSelectWhere(&leftExpr, false, expr)
	if err != nil {
		return "", err
	}

	rightStr, err := handleSelectWhere(&rightExpr, false, expr)
	if err != nil {
		return "", err
	}

	var resultStr string
	if leftStr == "" || rightStr == "" {
		resultStr = leftStr + rightStr
	} else {
		resultStr = leftStr + `,` + rightStr
	}

	// not toplevel
	// if the parent node is also or node, then merge the query param
	if _, ok := (*parent).(*sqlparser.OrExpr); ok {
		return resultStr, nil
	}

	return fmt.Sprintf(`{"bool" : {"should" : [%v]}}`, resultStr), nil
}

func buildComparisonExprRightStr(expr sqlparser.ValExpr) (string, error) {
	var rightStr string
	var err error
	switch expr.(type) {
	case sqlparser.StrVal:
		rightStr = sqlparser.String(expr)
		rightStr = strings.Trim(rightStr, `'`)
	case sqlparser.NumVal:
		rightStr = sqlparser.String(expr)
	case *sqlparser.FuncExpr:
		// parse nested
		funcExpr := expr.(*sqlparser.FuncExpr)
		rightStr, err = buildNestedFuncStrValue(funcExpr)
		if err != nil {
			return "", err
		}
	case *sqlparser.ColName:
		return "", errors.New("elasticsql: column name on the right side of compare operator is not supported")
	case sqlparser.ValTuple:
		rightStr = sqlparser.String(expr)
	default:
		// cannot reach here
	}
	return rightStr, err
}

func handleSelectWhereComparisonExpr(expr *sqlparser.BoolExpr, topLevel bool, parent *sqlparser.BoolExpr) (string, error) {
	comparisonExpr := (*expr).(*sqlparser.ComparisonExpr)
	colName, ok := comparisonExpr.Left.(*sqlparser.ColName)

	if !ok {
		return "", errors.New("elasticsql: invalid comparison expression, the left must be a column name")
	}

	colNameStr := sqlparser.String(colName)
	colNameStr = strings.Replace(colNameStr, "`", "", -1)
	rightStr, err := buildComparisonExprRightStr(comparisonExpr.Right)
	if err != nil {
		return "", err
	}

	resultStr := ""

	switch comparisonExpr.Operator {
	case ">=":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"from" : "%v"}}}`, colNameStr, rightStr)
	case "<=":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"to" : "%v"}}}`, colNameStr, rightStr)
	case "=":
		resultStr = fmt.Sprintf(`{"match" : {"%v" : {"query" : "%v", "type" : "phrase"}}}`, colNameStr, rightStr)
	case ">":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"gt" : "%v"}}}`, colNameStr, rightStr)
	case "<":
		resultStr = fmt.Sprintf(`{"range" : {"%v" : {"lt" : "%v"}}}`, colNameStr, rightStr)
	case "!=":
		resultStr = fmt.Sprintf(`{"bool" : {"must_not" : [{"match" : {"%v" : {"query" : "%v", "type" : "phrase"}}}]}}`, colNameStr, rightStr)
	case "in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		resultStr = fmt.Sprintf(`{"terms" : {"%v" : [%v]}}`, colNameStr, rightStr)
	case "like":
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		resultStr = fmt.Sprintf(`{"match" : {"%v" : {"query" : "%v", "type" : "phrase"}}}`, colNameStr, rightStr)
	case "not like":
		rightStr = strings.Replace(rightStr, `%`, ``, -1)
		resultStr = fmt.Sprintf(`{"bool" : {"must_not" : {"match" : {"%v" : {"query" : "%v", "type" : "phrase"}}}}}`, colNameStr, rightStr)
	case "not in":
		// the default valTuple is ('1', '2', '3') like
		// so need to drop the () and replace ' to "
		rightStr = strings.Replace(rightStr, `'`, `"`, -1)
		rightStr = strings.Trim(rightStr, "(")
		rightStr = strings.Trim(rightStr, ")")
		resultStr = fmt.Sprintf(`{"bool" : {"must_not" : {"terms" : {"%v" : [%v]}}}}`, colNameStr, rightStr)
	}

	// the root node need to have bool and must
	if topLevel {
		resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
	}

	return resultStr, nil
}

func handleSelectWhere(expr *sqlparser.BoolExpr, topLevel bool, parent *sqlparser.BoolExpr) (string, error) {
	if expr == nil {
		return "", errors.New("elasticsql: error expression cannot be nil here")
	}

	switch (*expr).(type) {
	case *sqlparser.AndExpr:
		return handleSelectWhereAndExpr(expr, topLevel, parent)

	case *sqlparser.OrExpr:
		return handleSelectWhereOrExpr(expr, topLevel, parent)
	case *sqlparser.ComparisonExpr:
		return handleSelectWhereComparisonExpr(expr, topLevel, parent)

	case *sqlparser.NullCheck:
		return "", errors.New("elasticsql: null check expression currently not supported")
	case *sqlparser.RangeCond:
		// between a and b
		// the meaning is equal to range query
		rangeCond := (*expr).(*sqlparser.RangeCond)
		colName, ok := rangeCond.Left.(*sqlparser.ColName)

		if !ok {
			return "", errors.New("elasticsql: range column name missing")
		}

		colNameStr := sqlparser.String(colName)
		fromStr := strings.Trim(sqlparser.String(rangeCond.From), `'`)
		toStr := strings.Trim(sqlparser.String(rangeCond.To), `'`)

		resultStr := fmt.Sprintf(`{"range" : {"%v" : {"from" : "%v", "to" : "%v"}}}`, colNameStr, fromStr, toStr)
		if topLevel {
			resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
		}

		return resultStr, nil

	case *sqlparser.ParenBoolExpr:
		parentBoolExpr := (*expr).(*sqlparser.ParenBoolExpr)
		boolExpr := parentBoolExpr.Expr

		// if paren is the top level, bool must is needed
		var isThisTopLevel = false
		if topLevel {
			isThisTopLevel = true
		}
		return handleSelectWhere(&boolExpr, isThisTopLevel, parent)
	case *sqlparser.NotExpr:
		return "", errors.New("elasticsql: not expression currently not supported")
	}

	return "", errors.New("elaticsql: logically cannot reached here")
}
