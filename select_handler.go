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

	queryFrom, querySize := "0", "1"

	aggFlag := false
	// if the request is to aggregation
	// then set aggFlag to true, and querySize to 0
	// to not return any query result

	var aggStr string
	if len(sel.GroupBy) > 0 {
		aggFlag = true
		querySize = "0"
		//fmt.Printf("%#v\n", sel.GroupBy)
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

func handleSelectWhere(expr *sqlparser.BoolExpr, topLevel bool, parent *sqlparser.BoolExpr) (string, error) {
	if expr == nil {
		return "", errors.New("elasticsql: error expression cannot be nil here")
	}

	switch (*expr).(type) {
	case *sqlparser.AndExpr:
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
		//fmt.Println("left is "+leftStr, "right is "+rightStr)

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
	case *sqlparser.OrExpr:
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
	case *sqlparser.ComparisonExpr:
		comparisonExpr := (*expr).(*sqlparser.ComparisonExpr)
		colName, ok := comparisonExpr.Left.(*sqlparser.ColName)

		if !ok {
			return "", errors.New("elasticsql: invalid comparison expression, the left must be a column name")
		}

		colNameStr := sqlparser.String(colName)
		rightStr := ""
		var err error
		switch comparisonExpr.Right.(type) {
		case sqlparser.StrVal:
			rightStr = sqlparser.String(comparisonExpr.Right)
			rightStr = strings.Trim(rightStr, `'`)
		case sqlparser.NumVal:
			rightStr = sqlparser.String(comparisonExpr.Right)
		case *sqlparser.FuncExpr:
			// parse nested
			funcExpr := comparisonExpr.Right.(*sqlparser.FuncExpr)
			rightStr, err = buildNestedFuncStrValue(funcExpr)
			if err != nil {
				return "", err
			}
		case *sqlparser.ColName:
			return "", errors.New("elasticsql: column name on the right side of compare operator is not supported")
		default:
			// cannot reach here
			// fmt.Printf("%#v", comparisonExpr.Right)
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
			return "", errors.New("elasticsql: not like currently not supported")
		}

		// the root node need to have bool and must
		if topLevel {
			resultStr = fmt.Sprintf(`{"bool" : {"must" : [%v]}}`, resultStr)
		}

		return resultStr, nil

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
		return handleSelectWhere(&boolExpr, false, parent)
	case *sqlparser.NotExpr:
		return "", errors.New("elasticsql: not expression currently not supported")
	}

	return "", errors.New("elaticsql: logically cannot reached here")
}

// extract func expressions from select exprs
func handleSelectSelect(sqlSelect sqlparser.SelectExprs) ([]*sqlparser.FuncExpr, []*sqlparser.ColName, error) {
	var colArr []*sqlparser.ColName
	var funcArr []*sqlparser.FuncExpr
	for _, v := range sqlSelect {
		// non star expressioin means column name
		// or some aggregation functions
		expr, ok := v.(*sqlparser.NonStarExpr)
		if !ok {
			// no need to handle, star expression * just skip is ok
			continue
		}

		// NonStarExpr start

		switch expr.Expr.(type) {
		case *sqlparser.FuncExpr:
			funcExpr := expr.Expr.(*sqlparser.FuncExpr)
			funcArr = append(funcArr, funcExpr)

		case *sqlparser.ColName:
			continue
		default:
			//ignore
			//fmt.Println("column not supported", sqlparser.String(expr.Expr))
		}

		//starExpression like *, table.* should be ignored
		//'cause it is meaningless to set fields in elasticsearch aggs
	}
	return funcArr, colArr, nil
}

// this function becomes too complicated, need refactor
func buildAggs(sel *sqlparser.Select) (string, error) {
	//the outer agg tree is built with the normal field extracted from group by
	//_, colNameArr, colErr := handleSelectGroupBy(sel.GroupBy)

	var aggMap = make(map[string]interface{})
	// point to the parent map value
	var parentNode *map[string]interface{}
	//for idx, v := range colNameArr {
	for idx, v := range sel.GroupBy {
		switch v.(type) {
		case *sqlparser.ColName:
			colName := v.(*sqlparser.ColName)
			if idx == 0 {
				innerMap := make(map[string]interface{})
				innerMap["terms"] = map[string]interface{}{
					"field": string(colName.Name),
					"size":  200, // this size may need to change ?
				}
				aggMap[string(colName.Name)] = innerMap
				parentNode = &innerMap
			} else {
				innerMap := make(map[string]interface{})
				innerMap["terms"] = map[string]interface{}{
					"field": string(colName.Name),
					"size":  0,
				}
				(*parentNode)["aggregations"] = map[string]interface{}{
					string(colName.Name): innerMap,
				}
				parentNode = &innerMap
			}
		case *sqlparser.FuncExpr:
			funcExpr := v.(*sqlparser.FuncExpr)
			// only handle the needed
			var field string
			interval := "1h"
			format := "yyyy-MM-dd HH:mm:ss"
			//fmt.Println(string(funcExpr.Name)) date_histogram
			if string(funcExpr.Name) == "date_histogram" {
				innerMap := make(map[string]interface{})
				//rightStr = strings.Replace(rightStr, `'`, `"`, -1)

				//get field/interval and format
				for _, expr := range funcExpr.Exprs {
					// the expression in date_histogram must be like a = b format
					switch expr.(type) {
					case *sqlparser.NonStarExpr:
						nonStarExpr := expr.(*sqlparser.NonStarExpr)
						comparisonExpr, ok := nonStarExpr.Expr.(*sqlparser.ComparisonExpr)
						if !ok {
							return "", errors.New("elasticsql: unsupported expression in date_histogram")
						}
						left, ok := comparisonExpr.Left.(*sqlparser.ColName)
						if !ok {
							return "", errors.New("elaticsql: param error in date_histogram")
						}
						rightStr := sqlparser.String(comparisonExpr.Right)
						rightStr = strings.Replace(rightStr, `'`, ``, -1)
						if string(left.Name) == "field" {
							field = rightStr
						}
						if string(left.Name) == "interval" {
							interval = rightStr
						}
						if string(left.Name) == "format" {
							format = rightStr
						}

					default:
						return "", errors.New("elasticsql: unsupported expression in date_histogram")
					}
				}

				innerMap["date_histogram"] = map[string]interface{}{
					"field":    field,
					"interval": interval,
					"format":   format,
				}
				keyName := sqlparser.String(funcExpr)
				keyName = strings.Replace(keyName, `'`, ``, -1)
				keyName = strings.Replace(keyName, ` `, ``, -1)
				aggMap[keyName] = innerMap
				parentNode = &innerMap
			}
		}
	}

	funcExprArr, _, funcErr := handleSelectSelect(sel.SelectExprs)

	// the final parentNode is the exact node
	// to nest the aggreagation functions
	// but v in loop all use the same parentNode
	var innerAggMap = make(map[string]interface{})
	if parentNode == nil {
		return "", errors.New("elasticsql: agg not supported yet")
	}

	for _, v := range funcExprArr {
		//func expressions will use the same parent bucket

		aggName := strings.ToUpper(string(v.Name)) + `(` + sqlparser.String(v.Exprs) + `)`
		switch string(v.Name) {
		case "count":
			//count need to distingush * and normal field name
			if sqlparser.String(v.Exprs) == "*" {
				innerAggMap[aggName] = map[string]interface{}{
					"value_count": map[string]string{
						"field": "_index",
					},
				}
			} else {
				innerAggMap[aggName] = map[string]interface{}{
					"value_count": map[string]string{
						"field": sqlparser.String(v.Exprs),
					},
				}
			}
		default:
			innerAggMap[aggName] = map[string]interface{}{
				string(v.Name): map[string]string{
					"field": sqlparser.String(v.Exprs),
				},
			}
		}

	}

	if len(innerAggMap) > 0 {
		(*parentNode)["aggregations"] = innerAggMap
	}

	mapJSON, _ := json.Marshal(aggMap)

	//if colErr == nil && funcErr == nil {
	if funcErr == nil {
	}

	return string(mapJSON), nil
}
