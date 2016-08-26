package elasticsql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func handleSelect(sel *sqlparser.Select) (dsl string, esType string, err error) {

	// Handle where
	// 顶层节点需要传一个空接口进去，用以判断父结点类型
	// 有没有更好的写法呢
	var rootParent sqlparser.BoolExpr
	var queryMap = `{"bool" : {"must": [{"match_all" : {}}]}}`
	//用户也有可能不传where条件
	if sel.Where != nil {
		queryMap = handleSelectWhere(&sel.Where.Expr, true, &rootParent)
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
	//if the request is to aggregation
	//then set aggFlag to true, and querySize to 0
	//to not return any query result
	//fmt.Printf("%#v\n", sel.GroupBy)
	//fmt.Printf("%#v\n", sel.SelectExprs)
	//fmt.Printf("%#v\n", len(sel.SelectExprs))

	if len(sel.GroupBy) > 0 {
		aggFlag = true
		querySize = "0"

		//聚集操作
		//先用列做出外层的aggregation
		colNameArr, err := handleSelectGroupBy(sel.GroupBy)
		fmt.Printf("%#v\n", colNameArr)
		fmt.Printf("%#v\n", len(colNameArr))
		if err != nil {
			//TODO
		}

		//然后用agg函数，做出最内层的aggregation
		funcExprArr, err := handleSelectSelect(sel.SelectExprs)
		fmt.Printf("%#v\n", funcExprArr)
		fmt.Printf("%#v\n", len(funcExprArr))
		if err != nil {
			//TODO
		}

	}

	//Handle limit
	if sel.Limit != nil {
		if sel.Limit.Offset != nil {
			queryFrom = sqlparser.String(sel.Limit.Offset)
		}
		querySize = sqlparser.String(sel.Limit.Rowcount)
	}

	//Handle order by
	//when executating aggregations, order by is useless
	var orderByArr []string
	if aggFlag == false {
		for _, orderByExpr := range sel.OrderBy {
			orderByStr := fmt.Sprintf(`{"%v": "%v"}`, sqlparser.String(orderByExpr.Expr), orderByExpr.Direction)
			orderByArr = append(orderByArr, orderByStr)
		}
	}

	resultMap := make(map[string]interface{})
	resultMap["query"] = queryMap
	resultMap["from"] = queryFrom
	resultMap["size"] = querySize

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

//TODO handle group by count having etc.
//for i, groupByExpr := range sel.GroupBy {
//	fmt.Printf("the %d of group by is %#v\n", i, sqlparser.String(groupByExpr))
//}

func handleSelectWhere(expr *sqlparser.BoolExpr, topLevel bool, parent *sqlparser.BoolExpr) string {
	//没有where条件
	if expr == nil {
		fmt.Println("error")
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
		if _, ok := (*parent).(*sqlparser.AndExpr); ok {
			return leftStr + `,` + rightStr
		}

		return fmt.Sprintf(`{"bool" : {"must" : [%v, %v]}}`, leftStr, rightStr)
	case *sqlparser.OrExpr:
		orExpr := (*expr).(*sqlparser.OrExpr)
		leftExpr := orExpr.Left
		rightExpr := orExpr.Right
		leftStr := handleSelectWhere(&leftExpr, false, expr)
		rightStr := handleSelectWhere(&rightExpr, false, expr)

		// not toplevel
		// if the parent node is also or node, then merge the query param
		if _, ok := (*parent).(*sqlparser.OrExpr); ok {
			return leftStr + `,` + rightStr
		}

		return fmt.Sprintf(`{"bool" : {"should" : [%v, %v]}}`, leftStr, rightStr)
	case *sqlparser.ComparisonExpr:
		comparisonExpr := (*expr).(*sqlparser.ComparisonExpr)
		colName, ok := comparisonExpr.Left.(*sqlparser.ColName)

		if !ok {
			fmt.Println("invalid comparison expr")
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

		//如果是root的话，需要加上query/bool和must
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

//从select里获取聚集函数
func handleSelectSelect(sqlSelect sqlparser.SelectExprs) ([]*sqlparser.FuncExpr, error) {
	var res []*sqlparser.FuncExpr
	for _, v := range sqlSelect {
		//fmt.Printf("%#v\n", v)
		//fmt.Printf("%#v\n", sqlparser.String(v))
		//non star expressioin means column name, or some aggregation functions
		expr, ok := v.(*sqlparser.NonStarExpr)
		if !ok {
			// no need to handle
			continue
		}

		// NonStarExpr start
		//fmt.Printf("%#v\n", sqlparser.String(expr.Expr))

		switch expr.Expr.(type) {
		case *sqlparser.FuncExpr:
			//fmt.Printf("%#v\n", funcExpr)
			// count(*)，这里拿到的是*
			// count(id)，这里拿到的是id
			//fmt.Printf("%#v\n", sqlparser.String(funcExpr.Exprs))
			//count/sum/min/avg/max
			//fmt.Printf("%#v\n", string(funcExpr.Name))
			funcExpr := expr.Expr.(*sqlparser.FuncExpr)
			res = append(res, funcExpr)

		case *sqlparser.ColName:
			//fmt.Printf("colname : %#v\n", colName.Name)
			continue
		default:
			fmt.Println("column not supported", sqlparser.String(expr.Expr))
		}

		//starExpression like *, table.* should be ignored
		//'cause it is meaningless to set fields in elasticsearch aggs
	}
	return res, nil
}

//从group by里获取bucket
func handleSelectGroupBy(sqlGroupBy sqlparser.GroupBy) ([]*sqlparser.ColName, error) {
	var res []*sqlparser.ColName
	for _, v := range sqlGroupBy {
		switch v.(type) {
		case *sqlparser.ColName:
			//fmt.Println("col name")
			colName := v.(*sqlparser.ColName)
			res = append(res, colName)
			//fmt.Println(string(colName.Name))
		case *sqlparser.FuncExpr:
			//fmt.Println("func expression")
			//funcExpr := v.(*sqlparser.FuncExpr)
			//fmt.Println(string(funcExpr.Name))
			//return nil, errors.New("group by aggregation function not supported")
			continue
		}
	}
	return res, nil
}

func buildAggs(cols []*sqlparser.ColName, aggFuncs []*sqlparser.FuncExpr) {
}
