package kvs

import (
	"database/sql"
	"iter"
	"time"

	"github.com/shamaton/msgpack/v2"
)

const schema = `
CREATE TABLE IF NOT EXISTS kvl_store (
kvl_key BLOB PRIMARY KEY,
kvl_value BLOB)
`

type Store[T any] struct {
	db *sql.DB
}

func Open[T any](sqlitedb *sql.DB, cleanup time.Duration) (store Store[T], err error) {
	store.db = sqlitedb
	if _, err = store.db.Exec(schema); err != nil {
		return
	}
	return
}

func (c Store[T]) Load(key []byte) (value T, ok bool) {
	row := c.db.QueryRow("SELECT kvl_value FROM kvl_store WHERE kvl_key = $1", key)
	blob := []byte{}
	if err := row.Scan(&blob); err != nil {
		return value, false
	}
	if err := msgpack.Unmarshal(blob, &value); err != nil {
		return value, false
	}
	return value, true
}
func (c Store[T]) LoadOrStore(key []byte, value T) (actual T, loaded bool) {
	if actual, loaded = c.Load(key); loaded {
		return actual, loaded
	}
	c.Store(key, value)
	return value, false
}
func (c Store[T]) LoadAndDelete(key []byte) (value T, loaded bool) {
	row := c.db.QueryRow("DELETE FROM kvl_store WHERE kvl_key = $1 RETURNING kvl_value", key)
	blob := []byte{}
	if err := row.Scan(&blob); err != nil {
		return value, false
	}
	if err := msgpack.Unmarshal(blob, &value); err != nil {
		return value, false
	}
	return value, true
}

func (c Store[T]) Delete(key []byte) error {
	_, err := c.db.Exec("DELETE FROM kvl_store WHERE kvl_key = $1", key)
	return err
}

func (c Store[T]) Store(key []byte, value T) error {
	blob, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	_, err = c.db.Exec("INSERT OR REPLACE INTO kvl_store(kvl_key, kvl_value) VALUES ($1, $2)", key, blob)
	return err
}

func (c Store[T]) Clear() error {
	_, err := c.db.Exec("DELETE FROM kvl_store")
	return err
}

func (c Store[T]) All() iter.Seq2[[]byte, T] {
	return func(yield func([]byte, T) bool) {
		rows, err := c.db.Query("SELECT kvl_key, kvl_value FROM kvl_store")
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

func (c Store[T]) Keys() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		rows, err := c.db.Query("SELECT kvl_key FROM kvl_store")
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

func (c Store[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		rows, err := c.db.Query("SELECT kvl_value FROM kvl_store")
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
