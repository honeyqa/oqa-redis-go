package main

import (
	"database/sql"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

func connectRedis() (c redis.Conn) {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	return c
}

func connectMysql() *sql.DB {
	db, err := sql.Open("mysql", "root:root@/oqa")
	if err != nil {
		panic(err)
	}
	return db
}

var redis_conn = connectRedis()
var mysql_db = connectMysql()

func main() {
	getShardData()
}

func getShardData() {
	var (
		id        int
		projectid string
		sharded   int
	)
	rows, err := mysql_db.Query("select * from test")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &projectid, &sharded)
		if err != nil {
			panic(err)
		}
		insertToRedis(projectid, sharded)
	}
}

func insertToRedis(k string, v int) {
	n, err := redis_conn.Do("HSET", "oqa_shard", k, v)
	if err != nil {
		panic(err)
	} else {
		log.Println(n, k, v)
	}
}
