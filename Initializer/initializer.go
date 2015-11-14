package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"os"
)

type Config struct {
	MySQL []string
}

func loadConfig() Config {
	file, _ := os.Open("config.json")
	decoder := json.NewDecoder(file)
	c := Config{}
	err := decoder.Decode(&c)
	if err != nil {
		fmt.Println("error:", err)
	}
	return c
}

// MySQL
func connectMysql(cfg Config) *sql.DB {
	db, err := sql.Open("mysql", cfg.MySQL[1]+":"+cfg.MySQL[2]+"@tcp("+cfg.MySQL[0]+")/"+cfg.MySQL[3])
	if err != nil {
		panic(err)
	}
	return db
}

// Redis
func connectRedis(cfg Config) (c redis.Conn) {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	return c
}

var config = loadConfig()
var conn_mysql = connectMysql(config)
var conn_redis = connectRedis(config)

func main() {
	getProjectData()
}

func getProjectData() {
	var (
		id  int
		api string
	)
	rows, err := conn_mysql.Query("select id,apikey from project")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &api)
		if err != nil {
			panic(err)
		}
		insertToRedis(api, id)
	}
}

func insertToRedis(k string, v int) {
	_, err := conn_redis.Do("HSET", "hqa_projects", k, v)
	if err != nil {
		insertToRedis(k, v)
	}
}
