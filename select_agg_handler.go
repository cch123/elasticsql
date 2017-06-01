package elasticsql

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func handleInnerAggMap(funcExprArr []*sqlparser.FuncExpr) map[string]interface{} {

	var innerAggMap = make(map[string]interface{})
	for _, v := range funcExprArr {
		//func expressions will use the same parent bucket

		aggName := strings.ToUpper(string(v.Name)) + `(` + sqlparser.String(v.Exprs) + `)`
		switch string(v.Name) {
		case "count":
			//count need to distinguish * and normal field name
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
			// support min/avg/max
			innerAggMap[aggName] = map[string]interface{}{
				string(v.Name): map[string]string{
					"field": sqlparser.String(v.Exprs),
				},
			}
		}

	}

	return innerAggMap

}

func handleGroupByColName(colName *sqlparser.ColName, index int) map[string]interface{} {
	innerMap := make(map[string]interface{})
	if index == 0 {
		innerMap["terms"] = map[string]interface{}{
			"field": string(colName.Name),
			"size":  200, // this size may need to change ?
		}
	} else {
		innerMap["terms"] = map[string]interface{}{
			"field": string(colName.Name),
			"size":  0,
		}
	}

	return innerMap
}

func handleGroupByFuncExpr(funcExpr *sqlparser.FuncExpr) (map[string]interface{}, error) {

	innerMap := make(map[string]interface{})
	switch string(funcExpr.Name) {
	case "date_histogram":
		var (
			// default
			field    = ""
			interval = "1h"
			format   = "yyyy-MM-dd HH:mm:ss"
		)

		//get field/interval and format
		for _, expr := range funcExpr.Exprs {
			// the expression in date_histogram must be like a = b format
			switch expr.(type) {
			case *sqlparser.NonStarExpr:
				nonStarExpr := expr.(*sqlparser.NonStarExpr)
				comparisonExpr, ok := nonStarExpr.Expr.(*sqlparser.ComparisonExpr)
				if !ok {
					return nil, errors.New("elasticsql: unsupported expression in date_histogram")
				}
				left, ok := comparisonExpr.Left.(*sqlparser.ColName)
				if !ok {
					return nil, errors.New("elaticsql: param error in date_histogram")
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

				innerMap["date_histogram"] = map[string]interface{}{
					"field":    field,
					"interval": interval,
					"format":   format,
				}
				return innerMap, nil
			default:
				return nil, errors.New("elasticsql: unsupported expression in date_histogram")
			}
		}
	}
	return nil, errors.New("elasticsql: not supported agg func yet")
}

func handleOuterAgg(groupBy sqlparser.GroupBy) (map[string]interface{}, *map[string]interface{}, error) {

	var aggMap = make(map[string]interface{})
	// point to the parent map value
	var parentNode *map[string]interface{}
	//for idx, v := range colNameArr {
	for idx, v := range groupBy {
		switch item := v.(type) {
		case *sqlparser.ColName:
			colName := v.(*sqlparser.ColName)
			innerMap := handleGroupByColName(item, idx)

			if idx == 0 {
				aggMap[string(colName.Name)] = innerMap
				parentNode = &innerMap
			} else {
				(*parentNode)["aggregations"] = map[string]interface{}{
					string(colName.Name): innerMap,
				}
				parentNode = &innerMap
			}

		case *sqlparser.FuncExpr:
			funcExpr := v.(*sqlparser.FuncExpr)
			innerMap, err := handleGroupByFuncExpr(item)
			if err != nil {
				return nil, nil, err
			}

			keyName := sqlparser.String(funcExpr)
			keyName = strings.Replace(keyName, `'`, ``, -1)
			keyName = strings.Replace(keyName, ` `, ``, -1)
			aggMap[keyName] = innerMap
			parentNode = &innerMap

		}
	}

	return aggMap, parentNode, nil
}

// this function becomes too complicated, need refactor
func buildAggs(sel *sqlparser.Select) (string, error) {

	//the outer agg tree is built with the normal field extracted from group by
	aggMap, parentNode, err := handleOuterAgg(sel.GroupBy)
	if err != nil {
		return "", err
	}

	funcExprArr, _, funcErr := extractFuncAndColFromSelect(sel.SelectExprs)

	// the final parentNode is the exact node
	// to nest the aggreagation functions
	// but v in loop all use the same parentNode
	if parentNode == nil {
		return "", errors.New("elasticsql: agg not supported yet")
	}

	innerAggMap := handleInnerAggMap(funcExprArr)

	if len(innerAggMap) > 0 {
		(*parentNode)["aggregations"] = innerAggMap
	}

	mapJSON, _ := json.Marshal(aggMap)

	//if colErr == nil && funcErr == nil {
	if funcErr == nil {
	}

	return string(mapJSON), nil
}

// extract func expressions from select exprs
func extractFuncAndColFromSelect(sqlSelect sqlparser.SelectExprs) ([]*sqlparser.FuncExpr, []*sqlparser.ColName, error) {
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
		}

		//starExpression like *, table.* should be ignored
		//'cause it is meaningless to set fields in elasticsearch aggs
	}
	return funcArr, colArr, nil
}
