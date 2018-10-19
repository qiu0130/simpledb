package simpledb

import "time"

type Misc interface {
	keys(key string) interface{}
	expire(key string) time.Duration
	delete(key string) bool
	object(key string) string
	ttl(key string) time.Duration
	exists(key string) bool
}
