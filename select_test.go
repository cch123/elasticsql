package elasticsql

import (
	"testing"

	"github.com/xwb1989/sqlparser"
)

//currently not support join syntax
var sqlArr = []string{
	//"select count(*), occupy from callcenter,bbb where id>0 and a=0 and process_id in (1, 2) and a.t = 1 and (b.c=2 or b.d=1) order by aaa desc, ddd asc  limit 1,2",
	"select occupy from ark_callcenter where process_id= 1",
	"select occupy from ark_callcenter where process_id > 1",
	"select occupy from ark_callcenter where process_id < 1",
	"select occupy from ark_callcenter where process_id <= 1",
	//注意，如果是字符串的话，需要把两边的单引号脱掉
	"select occupy from ark_callcenter where process_id >= '1'",
	"select occupy from ark_callcenter where process_id != 1",
	"select occupy from ark_callcenter where process_id = 0 and status= 1 and channel = 4",
	"select * from ark_callcenter where create_time between '2015-01-01 00:00:00' and '2015-01-01 00:00:00'",
	"select * from ark_callcenter where process_id > 1 and status = 1",
	"select * from ark_callcenter where process_id = 1",
	"select * from ark_callcenter where create_time between '2015-01-01T00:00:00+0800' and '2017-01-01T00:00:00+0800' and process_id = 0 and status >= 1 and content = '三个男人' and phone = '15810324322'",
	"select * from ark_callcenter where id > 1 or process_id = 0",
	"select * from ark_callcenter where id > 1 and d = 1 or process_id = 0 and x = 2",
	"select * from ark where id > 1 order by id asc, order_id desc",
	"select * from ark_callcenter where (id > 1 and d = 1)",
	"select * from ark_callcenter where (id > 1 and d = 1) or (c=1)",
	//"select * from aaa where not a = 1", //no support
	//"select * from aaa where not (a = 1)", //no support
	"select * from ark_callcenter where id > 1 or (process_id = 0)",
	"select * from ark_callcenter where id in (1,2,3,4)",
	"select * from ark_callcenter where id in ('232', '323') and content = 'aaaa'",
	//"select * from ark_callcenter where id in ('232', '323') and content like '%aaaa%'", not supported current
	//"select * from ark_callc where id not like 'aaa'",
	"select occupy from ark_callcenter where a != 1",
	"select occupy from ark_callcenter where a > '1'",
	//	"select occupy from ark_callcenter where a is null",
	//	"select occupy from ark_callcenter where a is not   null",
	"select occupy from ark_callcenter where a =1 and b = 2 and c=3",
	"select occupy from ark_callcenter where create_time between '2015-01-01 00:00:00' and '2014-02-02 00:00:00'",
	"select x from ark where a like '%a%'",
	//"select channel, count(*) as d from ark where d = 1 group by channel, count(*)",
	//"select id, count(*) from ark where d = 1 group by channel, count(*)",
	//"select id, count(id), count(*) from ark where d = 1 group by channel, id, process_id",
	//"SELECT sum(id),count(channel),avg(area_id),min(area_id), max(process_id), channel from g group by channel",
	"select count(*) from ark group by date_histogram(field='create_time', value='1h')",
	"select * from ark group by date_histogram(field='create_time', value='1h')",
	"select * from ark group by date_histogram(field='create_time', value='1h'), id",
}

func TestSelect(t *testing.T) {
	for _, sqlStr := range sqlArr {
		stmt, err := sqlparser.Parse(sqlStr)

		if err != nil {
			t.Error(err)
		}

		//sql valid, start to handle
		switch stmt.(type) {
		case *sqlparser.Select:
			_, _, _ = handleSelect(stmt.(*sqlparser.Select))
		case *sqlparser.Update:
			t.Error("select handler cannot handle update")
		case *sqlparser.Insert:
			t.Error("select handler cannot handle insert")
		case *sqlparser.Delete:
			t.Error("select handler cannot handle delete")
		}
	}

}
