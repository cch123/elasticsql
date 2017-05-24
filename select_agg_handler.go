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
			innerAggMap[aggName] = map[string]interface{}{
				string(v.Name): map[string]string{
					"field": sqlparser.String(v.Exprs),
				},
			}
		}

	}

	return innerAggMap

}

func handleOuterAgg(groupBy sqlparser.GroupBy) (map[string]interface{}, *map[string]interface{}, error) {

	var aggMap = make(map[string]interface{})
	// point to the parent map value
	var parentNode *map[string]interface{}
	//for idx, v := range colNameArr {
	for idx, v := range groupBy {
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
							return nil, nil, errors.New("elasticsql: unsupported expression in date_histogram")
						}
						left, ok := comparisonExpr.Left.(*sqlparser.ColName)
						if !ok {
							return nil, nil, errors.New("elaticsql: param error in date_histogram")
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
						return nil, nil, errors.New("elasticsql: unsupported expression in date_histogram")
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

	return aggMap, parentNode, nil
}

// this function becomes too complicated, need refactor
func buildAggs(sel *sqlparser.Select) (string, error) {
	//the outer agg tree is built with the normal field extracted from group by
	//_, colNameArr, colErr := handleSelectGroupBy(sel.GroupBy)

	aggMap, parentNode, err := handleOuterAgg(sel.GroupBy)
	if err != nil {
		return "", err
	}

	funcExprArr, _, funcErr := handleSelectSelect(sel.SelectExprs)

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
