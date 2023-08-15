package dbroute

import (
	"gorm.io/gorm"
	"time"
)

func SetMaxOpenConns(connPool gorm.ConnPool, maxOpen int) {
	if maxOpen != 0 {
		if conn, ok := connPool.(interface{ SetMaxOpenConns(int) }); ok {
			conn.SetMaxOpenConns(maxOpen)
		}
	}
}

func SetMaxIdleConns(connPool gorm.ConnPool, maxIdleConns int) {
	if maxIdleConns != 0 {
		if conn, ok := connPool.(interface{ SetMaxIdleConns(int) }); ok {
			conn.SetMaxIdleConns(maxIdleConns)
		}
	}
}

func SetConnMaxLifetime(connPool gorm.ConnPool, maxLifetime time.Duration) {
	if maxLifetime != 0 {
		if conn, ok := connPool.(interface{ SetConnMaxLifetime(time.Duration) }); ok {
			conn.SetConnMaxLifetime(maxLifetime)
		}
	}
}

func SetConnMaxIdleTime(connPool gorm.ConnPool, maxIdleTime time.Duration) {
	if maxIdleTime != 0 {
		if conn, ok := connPool.(interface{ SetConnMaxIdleTime(time.Duration) }); ok {
			conn.SetConnMaxIdleTime(maxIdleTime)
		}
	}
}
