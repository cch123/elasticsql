package elasticsql

import (
	"testing"

	"encoding/json"

	"reflect"

	"github.com/cch123/elasticsql/internal/github.com/xwb1989/sqlparser"
)

var unsupportedCaseList = []string{
	"insert into a values(1,2)",
	"update a set id = 1",
	"delete from a where id=1",
}

var selectCaseMap = map[string]string{
	//"select count(*), occupy from callcenter,bbb where id>0 and a=0 and process_id in (1, 2) and a.t = 1 and (b.c=2 or b.d=1) order by aaa desc, ddd asc  limit 1,2",
	"select occupy from ark where process_id= 1":                                                                                                                                                         `{"query" : {"bool" : {"must" : [{"match" : {"process_id" : {"query" : "1", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select occupy from ark where (process_id= 1)":                                                                                                                                                       `{"query" : {"bool" : {"must" : [{"match" : {"process_id" : {"query" : "1", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select occupy from ark where ((process_id= 1))":                                                                                                                                                     `{"query" : {"bool" : {"must" : [{"match" : {"process_id" : {"query" : "1", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select occupy from ark where (process_id = 1 and status=1)":                                                                                                                                         `{"query" : {"bool" : {"must" : [{"match" : {"process_id" : {"query" : "1", "type" : "phrase"}}},{"match" : {"status" : {"query" : "1", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select occupy from ark where process_id > 1":                                                                                                                                                        `{"query" : {"bool" : {"must" : [{"range" : {"process_id" : {"gt" : "1"}}}]}},"from" : 0,"size" : 1}`,
	"select occupy from ark where process_id < 1":                                                                                                                                                        `{"query" : {"bool" : {"must" : [{"range" : {"process_id" : {"lt" : "1"}}}]}},"from" : 0,"size" : 1}`,
	"select occupy from ark where process_id <= 1":                                                                                                                                                       `{"query" : {"bool" : {"must" : [{"range" : {"process_id" : {"to" : "1"}}}]}},"from" : 0,"size" : 1}`,
	"select occupy from ark_callcenter where process_id >= '1'":                                                                                                                                          `{"query" : {"bool" : {"must" : [{"range" : {"process_id" : {"from" : "1"}}}]}},"from" : 0,"size" : 1}`,
	"select occupy from ark_callcenter where process_id != 1":                                                                                                                                            `{"query" : {"bool" : {"must" : [{"bool" : {"must_not" : [{"match" : {"process_id" : {"query" : "1", "type" : "phrase"}}}]}}]}},"from" : 0,"size" : 1}`,
	"select occupy from ark_callcenter where process_id = 0 and status= 1 and channel = 4":                                                                                                               `{"query" : {"bool" : {"must" : [{"match" : {"process_id" : {"query" : "0", "type" : "phrase"}}},{"match" : {"status" : {"query" : "1", "type" : "phrase"}}},{"match" : {"channel" : {"query" : "4", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select * from ark_callcenter where create_time between '2015-01-01 00:00:00' and '2015-01-01 00:00:00'":                                                                                             `{"query" : {"bool" : {"must" : [{"range" : {"create_time" : {"from" : "2015-01-01 00:00:00", "to" : "2015-01-01 00:00:00"}}}]}},"from" : 0,"size" : 1}`,
	"select * from ark_callcenter where process_id > 1 and status = 1":                                                                                                                                   `{"query" : {"bool" : {"must" : [{"range" : {"process_id" : {"gt" : "1"}}},{"match" : {"status" : {"query" : "1", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select * from ark_callcenter where create_time between '2015-01-01T00:00:00+0800' and '2017-01-01T00:00:00+0800' and process_id = 0 and status >= 1 and content = '三个男人' and phone = '15810324322'": `{"query" : {"bool" : {"must" : [{"range" : {"create_time" : {"from" : "2015-01-01T00:00:00+0800", "to" : "2017-01-01T00:00:00+0800"}}},{"match" : {"process_id" : {"query" : "0", "type" : "phrase"}}},{"range" : {"status" : {"from" : "1"}}},{"match" : {"content" : {"query" : "三个男人", "type" : "phrase"}}},{"match" : {"phone" : {"query" : "15810324322", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select * from ark_callcenter where id > 1 or process_id = 0":                                                                                                                                        `{"query" : {"bool" : {"should" : [{"range" : {"id" : {"gt" : "1"}}},{"match" : {"process_id" : {"query" : "0", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select * from ark where id > 1 and d = 1 or process_id = 0 and x = 2":                                                                                                                               `{"query" : {"bool" : {"should" : [{"bool" : {"must" : [{"range" : {"id" : {"gt" : "1"}}},{"match" : {"d" : {"query" : "1", "type" : "phrase"}}}]}},{"bool" : {"must" : [{"match" : {"process_id" : {"query" : "0", "type" : "phrase"}}},{"match" : {"x" : {"query" : "2", "type" : "phrase"}}}]}}]}},"from" : 0,"size" : 1}`,
	"select * from ark where id > 1 order by id asc, order_id desc":                                                                                                                                      `{"query" : {"bool" : {"must" : [{"range" : {"id" : {"gt" : "1"}}}]}},"from" : 0,"size" : 1,"sort" : [{"id": "asc"},{"order_id": "desc"}]}`,
	"select * from ark where (id > 1 and d = 1)":                                                                                                                                                         `{"query" : {"bool" : {"must" : [{"range" : {"id" : {"gt" : "1"}}},{"match" : {"d" : {"query" : "1", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select * from ark where (id > 1 and d = 1) or (c=1)":                                                                                                                                                `{"query" : {"bool" : {"should" : [{"bool" : {"must" : [{"range" : {"id" : {"gt" : "1"}}},{"match" : {"d" : {"query" : "1", "type" : "phrase"}}}]}},{"match" : {"c" : {"query" : "1", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select * from ark where id > 1 or (process_id = 0)":                                                                                                                                                 `{"query" : {"bool" : {"should" : [{"range" : {"id" : {"gt" : "1"}}},{"match" : {"process_id" : {"query" : "0", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select * from ark where id in (1,2,3,4)":                                                                                                                                                            `{"query" : {"bool" : {"must" : [{"terms" : {"id" : [1, 2, 3, 4]}}]}},"from" : 0,"size" : 1}`,
	"select * from ark where id in ('232', '323') and content = 'aaaa'":                                                                                                                                  `{"query" : {"bool" : {"must" : [{"terms" : {"id" : ["232", "323"]}},{"match" : {"content" : {"query" : "aaaa", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select occupy from ark where create_time between '2015-01-01 00:00:00' and '2014-02-02 00:00:00'":                                                                                                   `{"query" : {"bool" : {"must" : [{"range" : {"create_time" : {"from" : "2015-01-01 00:00:00", "to" : "2014-02-02 00:00:00"}}}]}},"from" : 0,"size" : 1}`,
	"select x from ark where a like '%a%'":                                                                                                                                                               `{"query" : {"bool" : {"must" : [{"match" : {"a" : {"query" : "a", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select count(*) from ark group by date_histogram(field='create_time', value='1h')":                                                                                                                  `{"query" : {"bool" : {"must": [{"match_all" : {}}]}},"from" : 0,"size" : 0,"aggregations" : {"date_histogram(field=create_time,value=1h)":{"aggregations":{"COUNT(*)":{"value_count":{"field":"_index"}}},"date_histogram":{"field":"create_time","format":"yyyy-MM-dd HH:mm:ss","interval":"1h"}}}}`,
	"select * from ark group by date_histogram(field='create_time', value='1h')":                                                                                                                         `{"query" : {"bool" : {"must": [{"match_all" : {}}]}},"from" : 0,"size" : 0,"aggregations" : {"date_histogram(field=create_time,value=1h)":{"date_histogram":{"field":"create_time","format":"yyyy-MM-dd HH:mm:ss","interval":"1h"}}}}`,
	"select * from ark group by date_histogram(field='create_time', value='1h'), id":                                                                                                                     `{"query" : {"bool" : {"must": [{"match_all" : {}}]}},"from" : 0,"size" : 0,"aggregations" : {"date_histogram(field=create_time,value=1h)":{"aggregations":{"id":{"terms":{"field":"id","size":0}}},"date_histogram":{"field":"create_time","format":"yyyy-MM-dd HH:mm:ss","interval":"1h"}}}}`,
	"select * from ark where a like group_concat('%', 'abc', '%')":                                                                                                                                       `{"query" : {"bool" : {"must" : [{"match" : {"a" : {"query" : "abc", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
	"select * from `order`.abcd where `by` = 1":                                                                                                                                                          `{"query" : {"bool" : {"must" : [{"match" : {"by" : {"query" : "1", "type" : "phrase"}}}]}},"from" : 0,"size" : 1}`,
}

//currently not support join syntax
var sqlArr = []string{
	//"select count(*), occupy from callcenter,bbb where id>0 and a=0 and process_id in (1, 2) and a.t = 1 and (b.c=2 or b.d=1) order by aaa desc, ddd asc  limit 1,2",
	"select occupy from ark where process_id= 1",
	"select occupy from ark where (process_id= 1)",   // ?
	"select occupy from ark where ((process_id= 1))", // ?
	"select occupy from ark where (process_id = 1 and status=1)",
	"select occupy from ark where process_id > 1",
	"select occupy from ark where process_id < 1",
	"select occupy from ark where process_id <= 1",
	"select occupy from ark where process_id >= '1'",
	"select occupy from ark where process_id != 1",
	"select occupy from ark where process_id = 0 and status= 1 and channel = 4",
	"select * from ark where create_time between '2015-01-01 00:00:00' and '2015-01-01 00:00:00'",
	"select * from ark where process_id > 1 and status = 1",
	"select * from ark where process_id = 1",
	"select * from ark where create_time between '2015-01-01T00:00:00+0800' and '2017-01-01T00:00:00+0800' and process_id = 0 and status >= 1 and content = '三个男人' and phone = '15810324322'",
	"select * from ark where id > 1 or process_id = 0",
	"select * from ark where id > 1 and d = 1 or process_id = 0 and x = 2",
	"select * from ark where id > 1 order by id asc, order_id desc",
	"select * from ark where (id > 1 and d = 1)",
	"select * from ark where (id > 1 and d = 1) or (c=1)",
	//"select * from aaa where not a = 1", //no support
	//"select * from aaa where not (a = 1)", //no support
	"select * from ark where id > 1 or (process_id = 0)",
	"select * from ark where id in (1,2,3,4)",
	"select * from ark where id in ('232', '323') and content = 'aaaa'",
	//"select * from ark where id in ('232', '323') and content like '%aaaa%'", not supported current
	//"select * from ark_callc where id not like 'aaa'",
	"select occupy from ark where a != 1",
	"select occupy from ark where a > '1'",
	//	"select occupy from ark where a is null",
	//	"select occupy from ark where a is not   null",
	"select occupy from ark where a =1 and b = 2 and c=3",
	"select occupy from ark where create_time between '2015-01-01 00:00:00' and '2014-02-02 00:00:00'",
	"select x from ark where a like '%a%'",
	//"select channel, count(*) as d from ark where d = 1 group by channel, count(*)",
	//"select id, count(*) from ark where d = 1 group by channel, count(*)",
	//"select id, count(id), count(*) from ark where d = 1 group by channel, id, process_id",
	//"SELECT sum(id),count(channel),avg(area_id),min(area_id), max(process_id), channel from g group by channel",
	"select count(*) from ark group by date_histogram(field='create_time', value='1h')",
	"select * from ark group by date_histogram(field='create_time', value='1h')",
	"select * from ark group by date_histogram(field='create_time', value='1h'), id",
	"select * from ark where a like group_concat('%', 'abc', '%')",
	"select * from `order`.abcd where `by` = 1",
}

func TestSupported(t *testing.T) {
	for k, v := range selectCaseMap {
		var dslMap map[string]interface{}
		err := json.Unmarshal([]byte(v), &dslMap)
		if err != nil {
			t.Error("test case json unmarshal err!")
		}

		stmt, err := sqlparser.Parse(k)
		if err != nil {
			t.Error("parse sql error", k, err)
		}

		dsl, _, err := handleSelect(stmt.(*sqlparser.Select))
		var dslConvertedMap map[string]interface{}
		err = json.Unmarshal([]byte(dsl), &dslConvertedMap)

		if err != nil {
			t.Error("the generated dsl json unmarshal error!", k)
		}

		if !reflect.DeepEqual(dslMap, dslConvertedMap) {
			t.Error("the generated dsl is not equal to expected", k)
		}

	}
}

func TestUnsupported(t *testing.T) {
	for _, v := range unsupportedCaseList {
		var err error
		stmt, err := sqlparser.Parse(v)
		switch x := stmt.(type) {
		case *sqlparser.Update:
			_, _, err = handleUpdate(x)
		case *sqlparser.Delete:
			_, _, err = handleDelete(x)
		case *sqlparser.Insert:
			_, _, err = handleInsert(x)
		}

		if err == nil {
			t.Error("can not be true, these cases are not supported!")
		}

	}
}
