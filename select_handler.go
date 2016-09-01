package elasticsql

import (
	"encoding/json"
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
		queryMapStr = handleSelectWhere(&sel.Where.Expr, true, &rootParent)
		if queryMapStr == "" {
			queryMapStr = defaultQueryMapStr
		}
	}

	//TODO support multiple tables
	//for i, fromExpr := range sel.From {
	//	fmt.Printf("the %d of from is %#v\n", i, sqlparser.String(fromExpr))
	//}

	//Handle from
	if len(sel.From) != 1 {
		return "", "", errors.New("multiple from currently not supported")
	}
	esType = sqlparser.String(sel.From)

	queryFrom, querySize := "0", "1"

	aggFlag := false
	// if the request is to aggregation
	// then set aggFlag to true, and querySize to 0
	// to not return any query result

	var aggStr string
	var aggBuildErr error
	if len(sel.GroupBy) > 0 {
		aggFlag = true
		querySize = "0"
		aggStr, aggBuildErr = buildAggs(sel)
		if aggBuildErr != nil {
			aggStr = ""
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

	var resultArr []string
	for key, val := range resultMap {
		resultArr = append(resultArr, fmt.Sprintf(`"%v" : %v`, key, val))
	}

	dsl = "{" + strings.Join(resultArr, ",") + "}"
	return dsl, esType, nil
}

func handleSelectWhere(expr *sqlparser.BoolExpr, topLevel bool, parent *sqlparser.BoolExpr) string {
	if expr == nil {
		//fmt.Println("error")
		return ""
	}

	switch (*expr).(type) {
	case *sqlparser.AndExpr:
		andExpr := (*expr).(*sqlparser.AndExpr)
		leftExpr := andExpr.Left
		rightExpr := andExpr.Right
		leftStr := handleSelectWhere(&leftExpr, false, expr)
		rightStr := handleSelectWhere(&rightExpr, false, expr)

		// not toplevel
		// if the parent node is also and, then the result can be merged
		//fmt.Println("left is "+leftStr, "right is "+rightStr)

		var resultStr string
		if leftStr == "" || rightStr == "" {
			resultStr = leftStr + rightStr
		} else {
			resultStr = leftStr + `,` + rightStr
		}

		if _, ok := (*parent).(*sqlparser.AndExpr); ok {
			return resultStr
		}

		return fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
	case *sqlparser.OrExpr:
		orExpr := (*expr).(*sqlparser.OrExpr)
		leftExpr := orExpr.Left
		rightExpr := orExpr.Right
		leftStr := handleSelectWhere(&leftExpr, false, expr)
		rightStr := handleSelectWhere(&rightExpr, false, expr)

		var resultStr string
		if leftStr == "" || rightStr == "" {
			resultStr = leftStr + rightStr
		} else {
			resultStr = leftStr + `,` + rightStr
		}

		// not toplevel
		// if the parent node is also or node, then merge the query param
		if _, ok := (*parent).(*sqlparser.OrExpr); ok {
			return resultStr
		}

		return fmt.Sprintf(`{"bool" : {"should" : [%v]}}`, resultStr)
	case *sqlparser.ComparisonExpr:
		comparisonExpr := (*expr).(*sqlparser.ComparisonExpr)
		colName, ok := comparisonExpr.Left.(*sqlparser.ColName)

		if !ok {
			fmt.Println("error, left of comparison expression must be column name")
			return ""
		}

		colNameStr := sqlparser.String(colName)
		rightStr := sqlparser.String(comparisonExpr.Right)
		rightStr = strings.Trim(rightStr, `'`)

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
		}

		// the root node need to have bool and must
		if topLevel {
			resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
		}

		return resultStr

	case *sqlparser.NullCheck:
		fmt.Println("null check expr, currently will not handle", expr)
	case *sqlparser.RangeCond:
		// between a and b
		// the meaning is equal to range query
		rangeCond := (*expr).(*sqlparser.RangeCond)
		colName, ok := rangeCond.Left.(*sqlparser.ColName)

		if !ok {
			fmt.Println("range column name missing")
			return ""
		}

		colNameStr := sqlparser.String(colName)
		fromStr := strings.Trim(sqlparser.String(rangeCond.From), `'`)
		toStr := strings.Trim(sqlparser.String(rangeCond.To), `'`)

		resultStr := fmt.Sprintf(`{"range" : {"%v" : {"from" : "%v", "to" : "%v"}}}`, colNameStr, fromStr, toStr)
		if topLevel {
			resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
		}

		return resultStr

	case *sqlparser.ParenBoolExpr:
		parentBoolExpr := (*expr).(*sqlparser.ParenBoolExpr)
		boolExpr := parentBoolExpr.Expr
		return handleSelectWhere(&boolExpr, false, parent)
	case *sqlparser.NotExpr:
		fmt.Println("not expr, todo handle")
	}
	return ""
}

// extract func expressions from select exprs
func handleSelectSelect(sqlSelect sqlparser.SelectExprs) ([]*sqlparser.FuncExpr, error) {
	var res []*sqlparser.FuncExpr
	for _, v := range sqlSelect {
		// non star expressioin means column name
		// or some aggregation functions
		expr, ok := v.(*sqlparser.NonStarExpr)
		if !ok {
			// no need to handle
			continue
		}

		// NonStarExpr start

		switch expr.Expr.(type) {
		case *sqlparser.FuncExpr:
			funcExpr := expr.Expr.(*sqlparser.FuncExpr)
			res = append(res, funcExpr)

		case *sqlparser.ColName:
			continue
		default:
			fmt.Println("column not supported", sqlparser.String(expr.Expr))
		}

		//starExpression like *, table.* should be ignored
		//'cause it is meaningless to set fields in elasticsearch aggs
	}
	return res, nil
}

// extract colnames from group by
func handleSelectGroupBy(sqlGroupBy sqlparser.GroupBy) ([]*sqlparser.ColName, error) {
	var res []*sqlparser.ColName
	for _, v := range sqlGroupBy {
		switch v.(type) {
		case *sqlparser.ColName:
			colName := v.(*sqlparser.ColName)
			res = append(res, colName)
		case *sqlparser.FuncExpr:
			continue
		}
	}
	return res, nil
}

func buildAggs(sel *sqlparser.Select) (string, error) {
	//the outer agg tree is built with the normal field extracted from group by
	colNameArr, colErr := handleSelectGroupBy(sel.GroupBy)

	var aggMap = make(map[string]interface{})
	// point to the parent map value
	var parentNode *map[string]interface{}
	for idx, v := range colNameArr {
		if idx == 0 {
			innerMap := make(map[string]interface{})
			innerMap["terms"] = map[string]interface{}{
				"field": string(v.Name),
				"size":  200,
			}
			aggMap[string(v.Name)] = innerMap
			parentNode = &innerMap
		} else {
			innerMap := make(map[string]interface{})
			innerMap["terms"] = map[string]interface{}{
				"field": string(v.Name),
				"size":  0,
			}
			(*parentNode)["aggregations"] = map[string]interface{}{
				string(v.Name): innerMap,
			}
			parentNode = &innerMap
		}
	}

	funcExprArr, funcErr := handleSelectSelect(sel.SelectExprs)

	// the final parentNode is the exact node
	// to nest the aggreagation functions
	// but v in loop all use the same parentNode
	var innerAggMap = make(map[string]interface{})
	(*parentNode)["aggregations"] = innerAggMap
	parentNode = &innerAggMap

	for _, v := range funcExprArr {
		//func expressions will use the same parent bucket

		aggName := strings.ToUpper(string(v.Name)) + `(` + sqlparser.String(v.Exprs) + `)`
		switch string(v.Name) {
		case "count":
			//count need to distingush * and normal field name
			if sqlparser.String(v.Exprs) == "*" {
				(*parentNode)[aggName] = map[string]interface{}{
					"value_count": map[string]string{
						"field": "_index",
					},
				}
			} else {
				(*parentNode)[aggName] = map[string]interface{}{
					"value_count": map[string]string{
						"field": sqlparser.String(v.Exprs),
					},
				}
			}
		default:
			(*parentNode)[aggName] = map[string]interface{}{
				string(v.Name): map[string]string{
					"field": sqlparser.String(v.Exprs),
				},
			}
		}

	}

	mapJSON, _ := json.Marshal(aggMap)

	if colErr == nil && funcErr == nil {
	}

	return string(mapJSON), nil
}
