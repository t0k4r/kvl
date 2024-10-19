package kvl

import (
	"database/sql"
	"iter"

	"github.com/shamaton/msgpack/v2"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS kvl_store (
kvl_key TEXT PRIMARY KEY,
kvl_value BLOB);
`

type Store[T any] struct {
	db *sql.DB
}

func Open[T any](name string) (store Store[T], err error) {
	if store.db, err = sql.Open("sqlite", name); err != nil {
		return
	}
	if _, err = store.db.Exec(schema); err != nil {
		return
	}
	return
}
func (s *Store[T]) Load(key string) (value T, ok bool) {
	row := s.db.QueryRow("SELECT kvl_value FROM kvl_store WHERE kvl_key=$1", key)
	if err := row.Err(); err != nil {
		return
	}
	blob := []byte{}
	if err := row.Scan(&blob); err != nil {
		return
	}
	if err := msgpack.Unmarshal(blob, &value); err != nil {
		return
	}
	return value, true
}
func (s *Store[T]) Store(key string, value T) error {
	blob, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("INSERT OR REPLACE INTO kvl_store(kvl_key,kvl_value) values ($1,$2)", key, blob)
	return err
}
func (s *Store[T]) Delete(key string) error {
	_, err := s.db.Exec("DELETE FROM kvl_store WHERE kvl_key=$1", key)
	return err
}

func (s *Store[T]) All() iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		rows, err := s.db.Query("SELECT kvl_key, kvl_value FROM kvl_stores")
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			var key string
			var value T
			if err := rows.Scan(&key, &value); err != nil {
				return
			}
			if !yield(key, value) {
				return
			}
		}
	}
}
func (s *Store[T]) Keys() iter.Seq[string] {
	return func(yield func(string) bool) {
		rows, err := s.db.Query("SELECT kvl_key FROM kvl_stores")
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				return
			}
			if !yield(key) {
				return
			}
		}
	}
}
func (s *Store[T]) Values() iter.Seq[T] {
	return func(yield func(T) bool) {
		rows, err := s.db.Query("SELECT kvl_value FROM kvl_stores")
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			var value T
			if err := rows.Scan(&value); err != nil {
				return
			}
			if !yield(value) {
				return
			}
		}
	}
}
