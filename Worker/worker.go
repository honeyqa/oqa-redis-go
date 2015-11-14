// Worker for insert session data to MySQL every 15min
package main

import (
	"database/sql"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron"
	"log"
	"os"
	"strconv"
	"time"
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
		log.Println("error:", err)
	}
	return c
}

// MySQL
func connectMysql(cfg Config) *sql.DB {
	db, err := sql.Open("mysql", cfg.MySQL[1]+":"+cfg.MySQL[2]+"@tcp("+cfg.MySQL[0]+")/"+cfg.MySQL[3])
	if err != nil {
		panic(err)
	}
	db.SetMaxIdleConns(0)
	return db
}

// Redis Pool
// https://github.com/garyburd/redigo/blob/master/redis/pool.go#L51
func connectRedis(host, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

var config = loadConfig()
var mysqlPool = connectMysql(config)
var redisPool = connectRedis(":6379", "")

func main() {
	defer mysqlPool.Close()
	c := cron.New()
	c.AddFunc("0 0,15,30,45 * * *", func() {
		redis_conn := redisPool.Get()
		defer redis_conn.Close() // Connection must be closed after using
		var m int = time.Now().Minute() / 15
		m = m - 1
		if m < 0 {
			m = 3
		}
		r, _ := redis.Ints(redis_conn.Do("HGETALL", "hqa_session_"+strconv.Itoa(m)))
		for i := 0; i < len(r)/2; i++ {
			// project id : r[i*2] (int)
			// session count : r[i*2 + 1] (int)
			tx, err := mysqlPool.Begin()
			if err != nil {
				log.Fatal(err)
			}
			defer tx.Rollback()
			stmt, err := tx.Prepare("INSERT INTO sessionruncount (datetime, count, project_id) VALUES (NOW(), ?, ?)")
			if err != nil {
				log.Fatal(err)
			}
			defer stmt.Close()
			_, err = stmt.Exec(r[i*2], r[i*2+1])
			if err != nil {
				log.Fatal(err)
			}
			err = tx.Commit()
			if err != nil {
				log.Fatal(err)
			}
		}
		_, err := redis_conn.Do("DEL", "hqa_session_"+strconv.Itoa(m))
		if err != nil {
			log.Println(err)
		}
	})
	c.Start()
	for {

	}
}
