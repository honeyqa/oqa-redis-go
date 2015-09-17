package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
)

func connectRedis() (c redis.Conn) {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	return c
}

func connectMysql() (c gorm.DB) {
	db, err := gorm.Open("mysql", "root:root@/oqa")
	if err != nil {
		panic(err)
	}
	return db
}

var redis_conn = connectRedis()
var mysql_conn = connectMysql()

func main() {
	insertToRedis(0)
}

func getShardData() {}

func insertToRedis(i int) {
	n, err := redis_conn.Do("HSET", "oqa_shard", "key", "value")
	if err != nil {
		panic(err)
	} else {
		fmt.Println(n)
	}
}
