This tool converts sql to elasticsearch dsl

Currently support:

```
[x]sql and expression
[x]sql or expression
[x]equal(=) support
[x]not equal(!=) support
[x]gt(>) support
[x]gte(>=) support
[x]lt(<) support
[x]lte(<=) support
[x]sql in (eg. id in (1,2,3) ) expression
[x]paren bool support (eg. where (a=1 or b=1) and (c=1 or d=1))
[ ]sql like expression
[ ]sql not like expression
[ ]null check expression(is null/is not null)
[ ]join expression
[ ]aggregation like count(*)/group by/having support
```

*Improve : now the query DSL is much more flat*

##Usage

> go get github.com/cch123/elasticsql

Demo :
```go
package main

import (
    "fmt"

    "github.com/cch123/elasticsql"
)

func main() {
    res, _ := elasticsql.Convert("select * from aaa where a=1 and x = '三个男人' and create_time between '2015-01-01T00:00:00+0800' and '2016-01-01T00:00:00+0800' and process_id > 1 order by id desc limit 100,10")
    fmt.Println(res)
}

```
will produce :
```json
{
    "sort": [
        {
            "id": "desc"
        }
    ],
    "query": {
        "bool": {
            "must": [
                {
                    "match": {
                        "a": {
                            "query": "1",
                            "type": "phrase"
                        }
                    }
                },
                {
                    "match": {
                        "x": {
                            "query": "三个男人",
                            "type": "phrase"
                        }
                    }
                },
                {
                    "range": {
                        "create_time": {
                            "from": "2015-01-01T00:00:00+0800",
                            "to": "2016-01-01T00:00:00+0800"
                        }
                    }
                },
                {
                    "range": {
                        "process_id": {
                            "gt": "1"
                        }
                    }
                }
            ]
        }
    },
    "from": 100,
    "size": 10
}
```
