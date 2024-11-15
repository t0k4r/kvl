package kvc

import (
	"database/sql"
	"iter"
	"time"

	"github.com/shamaton/msgpack/v2"
)

const schema = `
CREATE TABLE IF NOT EXISTS kvl_cache (
kvl_key BLOB PRIMARY KEY,
kvl_value BLOB,
kvl_cleanup INTEGER)
`

type Cache[T any] struct {
	db    *sql.DB
	timer *time.Timer
}

func Open[T any](sqlitedb *sql.DB, cleanup time.Duration) (cache Cache[T], err error) {
	cache.db = sqlitedb
	if _, err = cache.db.Exec(schema); err != nil {
		return
	}
	if err = cache.cleanup(); err != nil {
		return
	}
	cache.timer = time.AfterFunc(cleanup, func() { cache.cleanup() })
	return
}

func (c Cache[T]) cleanup() error {
	_, err := c.db.Exec(`DELETE FROM kvl_cache WHERE kvl_cleanup < unixepoch()`)
	return err
}

func (c Cache[T]) Load(key []byte) (value T, ok bool) {
	row := c.db.QueryRow("SELECT kvl_value FROM kvl_cache WHERE  kvl_cleanup > unixepoch() AND kvl_key = $1", key)
	blob := []byte{}
	if err := row.Scan(&blob); err != nil {
		return value, false
	}
	if err := msgpack.Unmarshal(blob, &value); err != nil {
		return value, false
	}
	return value, true
}
func (c Cache[T]) LoadOrStore(key []byte, value T, duration time.Duration) (actual T, loaded bool) {
	if actual, loaded = c.Load(key); loaded {
		return actual, loaded
	}
	c.Store(key, value, duration)
	return value, false
}
func (c Cache[T]) LoadAndDelete(key []byte) (value T, loaded bool) {
	row := c.db.QueryRow("DELETE FROM kvl_cache WHERE  kvl_cleanup > unixepoch() AND kvl_key = $1 RETURNING kvl_value", key)
	blob := []byte{}
	if err := row.Scan(&blob); err != nil {
		return value, false
	}
	if err := msgpack.Unmarshal(blob, &value); err != nil {
		return value, false
	}
	return value, true
}

func (c Cache[T]) Delete(key []byte) error {
	_, err := c.db.Exec("DELETE FROM kvl_cache WHERE kvl_key = $1", key)
	return err
}

func (c Cache[T]) Store(key []byte, value T, duration time.Duration) error {
	blob, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	_, err = c.db.Exec("INSERT OR REPLACE INTO kvl_cache(kvl_key, kvl_value, kvl_cleanup) VALUES ($1, $2, $3)", key, blob, time.Now().Add(duration).Unix())
	return err
}

func (c Cache[T]) Clear() error {
	_, err := c.db.Exec("DELETE FROM kvl_cache")
	return err
}

func (c Cache[T]) All() iter.Seq2[[]byte, T] {
	return func(yield func([]byte, T) bool) {
		rows, err := c.db.Query("SELECT kvl_key, kvl_value FROM kvl_cache WHERE kvl_cleanup > unixepoch()")
		if err != nil {
			return
		}
		for rows.Next() {
			key := []byte{}
			blob := []byte{}
			if err = rows.Scan(&key, &blob); err != nil {
				return
			}
			var value T
			if err := msgpack.Unmarshal(blob, &value); err != nil {
				return
			}
			if !yield(key, value) {
				return
			}
		}
	}
}

func (c Cache[T]) Keys() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rows, err := c.db.Query("SELECT kvl_key FROM kvl_cache WHERE kvl_cleanup > unixepoch()")
		if err != nil {
			return
		}
		for rows.Next() {
			key := []byte{}
			if err = rows.Scan(&key); err != nil {
				return
			}
			if !yield(key) {
				return
			}
		}
	}
}

func (c Cache[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		rows, err := c.db.Query("SELECT kvl_value FROM kvl_cache WHERE kvl_cleanup > unixepoch()")
		if err != nil {
			return
		}
		for rows.Next() {
			blob := []byte{}
			if err = rows.Scan(&blob); err != nil {
				return
			}
			var value T
			if err := msgpack.Unmarshal(blob, &value); err != nil {
				return
			}
			if !yield(value) {
				return
			}
		}
	}
}
