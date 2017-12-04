```
 _____ _         _     ____ _____ ___  ____  ____   ___   _
| ____| |       / \   / ___|_   _|_ _|/ ___|/ ___| / _ \ | |
|  _| | |      / _ \  \___ \ | |  | || |    \___ \| | | || |
| |___| |___  / ___ \  ___) || |  | || |___  ___) | |_| || |___
|_____|_____|/_/   \_\|____/ |_| |___|\____||____/ \__\_\|_____|
```

Overview
-----------
[![Build Status](https://travis-ci.org/cch123/elasticsql.svg?branch=master)](https://travis-ci.org/cch123/elasticsql)
[![Go Documentation](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://godoc.org/github.com/cch123/elasticsql)
[![Coverage Status](https://coveralls.io/repos/github/cch123/elasticsql/badge.svg?branch=master)](https://coveralls.io/github/cch123/elasticsql?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/cch123/elasticsql)](https://goreportcard.com/report/github.com/cch123/elasticsql)



This tool converts sql to elasticsearch dsl

Currently support:

- [x] sql and expression
- [x] sql or expression
- [x] equal(=) support
- [x] not equal(!=) support
- [x] gt(>) support
- [x] gte(>=) support
- [x] lt(<) support
- [x] lte(<=) support
- [x] sql in (eg. id in (1,2,3) ) expression
- [x] sql not in (eg. id not in (1,2,3) ) expression
- [x] paren bool support (eg. where (a=1 or b=1) and (c=1 or d=1))
- [x] sql like expression (currently use match phrase, perhaps will change to wildcard in the future)
- [x] sql order by support
- [x] sql limit support
- [x] sql not like expression
- [x] field missing check
- [x] support aggregation like count(\*), count(field), min(field), max(field), avg(field)
- [x] support aggregation like stats(field), extended_stats(field), percentiles(field) which are not standard sql function
- [ ] null check expression(is null/is not null)
- [ ] join expression
- [ ] having support

Usage
-------------

> go get -u github.com/cch123/elasticsql

Demo :
```go
package main

import (
    "fmt"

    "github.com/cch123/elasticsql"
)

var sql = `
select * from aaa
where a=1 and x = '三个男人'
and create_time between '2015-01-01T00:00:00+0800' and '2016-01-01T00:00:00+0800'
and process_id > 1 order by id desc limit 100,10
`

func main() {
    dsl, esType, _ := elasticsql.Convert(sql)
    fmt.Println(dsl)
    fmt.Println(esType)
}

```
will produce :
```json
{
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
    "size": 10,
    "sort": [
        {
            "id": "desc"
        }
    ]
}

aaa
```

If your sql contains some keywords, eg. order, timestamp, don't forget to escape these fields as follows:

```
select * from `order` where `timestamp` = 1 and `desc`.id > 0
```

Warning
------------
To use this tool, you need to understand the term query and match phrase query of elasticsearch.

Setting a field to analyzed or not analyzed will get different results.

Details
------------
For more details of convertion, please refer to the [wiki](https://github.com/cch123/elasticsql/wiki)

Other info
------------
When writing this tool, I tried to avoid the deprecated dsl filters and aggregations, so it is compatible with most versions of the elasticsearch

If you have any advices or ideas, welcome to submit an issue or Pull Request!

License
-----------
MIT
