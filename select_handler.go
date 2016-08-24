package elasticsql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func handleSelect(sel *sqlparser.Select) string {

	// 顶层节点需要传一个空接口进去，用以判断父结点类型
	// 有没有更好的写法呢
	var rootParent sqlparser.BoolExpr
	queryMap := handleSelectWhere(&sel.Where.Expr, true, &rootParent)

	// from means the index and the type
	//for i, fromExpr := range sel.From {
	//	fmt.Printf("the %d of from is %#v\n", i, sqlparser.String(fromExpr))
	//}

	queryFrom, querySize := "0", "1"

	if sel.Limit != nil {
		queryFrom = sqlparser.String(sel.Limit.Offset)
		querySize = sqlparser.String(sel.Limit.Rowcount)
	}

	var orderByArr []string
	orderByStr := ""
	for _, orderByExpr := range sel.OrderBy {
		orderByStr = fmt.Sprintf(`{"%v": "%v"}`, sqlparser.String(orderByExpr.Expr), orderByExpr.Direction)
		orderByArr = append(orderByArr, orderByStr)
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

	resultStr := "{" + strings.Join(resultArr, ",") + "}"

	//fmt.Println(resultStr)
	return resultStr

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
	//fmt.Printf("%#v\n", (*expr))

	switch (*expr).(type) {
	case *sqlparser.AndExpr:
		andExpr := (*expr).(*sqlparser.AndExpr)
		//fmt.Println("and expr", expr)
		leftExpr := andExpr.Left
		rightExpr := andExpr.Right
		//fmt.Printf("%#v\n", leftExpr)
		//fmt.Printf("%#v\n", rightExpr)
		leftStr := handleSelectWhere(&leftExpr, false, expr)
		rightStr := handleSelectWhere(&rightExpr, false, expr)

		// not toplevel
		// 如果父节点也是and，那么结果可以直接和父结果进行合并
		if _, ok := (*parent).(*sqlparser.AndExpr); ok {
			return leftStr + `,` + rightStr
		}

		return fmt.Sprintf(`{"bool" : {"must" : [%v, %v]}}`, leftStr, rightStr)
	case *sqlparser.OrExpr:
		orExpr := (*expr).(*sqlparser.OrExpr)
		//fmt.Println("or expr", expr)
		leftExpr := orExpr.Left
		rightExpr := orExpr.Right
		//fmt.Printf("%#v\n", leftExpr)
		//fmt.Printf("%#v\n", rightExpr)
		leftStr := handleSelectWhere(&leftExpr, false, expr)
		rightStr := handleSelectWhere(&rightExpr, false, expr)

		// not toplevel
		// 如果父节点也是or，那么结果可以直接和父结果进行合并
		if _, ok := (*parent).(*sqlparser.OrExpr); ok {
			return leftStr + `,` + rightStr
		}

		return fmt.Sprintf(`{"bool" : {"should" : [%v, %v]}}`, leftStr, rightStr)
	case *sqlparser.ComparisonExpr:
		//fmt.Println("comparison expr", expr)
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
		//fmt.Println(comparisonExpr.Operator)
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
		//between a and b
		//翻译过来其实也是个range查询
		//fmt.Println("range condition expr", expr)
		rangeCond := (*expr).(*sqlparser.RangeCond)
		//fmt.Printf("%#v\n", rangeCond)
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
		//fmt.Printf("%#v\n", boolExpr)
		return handleSelectWhere(&boolExpr, false, parent)
	case *sqlparser.NotExpr:
		fmt.Println("not expr, todo handle")
	}
	return ""
}
