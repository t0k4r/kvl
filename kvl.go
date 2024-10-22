package kvl

import (
	"database/sql"
	"iter"

	"github.com/shamaton/msgpack/v2"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS kvl_store (
kvl_key BLOB PRIMARY KEY,
kvl_value BLOB);
`

type Store[K, V any] struct {
	db *sql.DB
}

func Open[K, V any](name string) (store Store[K, V], err error) {
	if store.db, err = sql.Open("sqlite", name); err != nil {
		return
	}
	if _, err = store.db.Exec(schema); err != nil {
		return
	}
	return
}
func (s *Store[K, V]) Close() error {
	return s.db.Close()
}

func (s *Store[K, V]) Load(key K) (value V, ok bool) {
	keyBlob, err := msgpack.Marshal(key)
	if err != nil {
		return
	}
	row := s.db.QueryRow("SELECT kvl_value FROM kvl_store WHERE kvl_key=$1", keyBlob)
	if err := row.Err(); err != nil {
		return
	}
	valueBlob := []byte{}
	if err := row.Scan(&valueBlob); err != nil {
		return
	}
	if err := msgpack.Unmarshal(valueBlob, &value); err != nil {
		return
	}
	return value, true
}
func (s *Store[K, V]) Store(key K, value V) error {
	keyBlob, err := msgpack.Marshal(key)
	if err != nil {
		return err
	}
	valueBlob, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("INSERT OR REPLACE INTO kvl_store(kvl_key,kvl_value) values ($1,$2)", keyBlob, valueBlob)
	return err
}
func (s *Store[K, V]) Delete(key K) error {
	keyBlob, err := msgpack.Marshal(key)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("DELETE FROM kvl_store WHERE kvl_key=$1", keyBlob)
	return err
}

func (s *Store[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		rows, err := s.db.Query("SELECT kvl_key, kvl_value FROM kvl_store")
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			keyBlob := []byte{}
			valueBlob := []byte{}
			if err := rows.Scan(&keyBlob, &valueBlob); err != nil {
				return
			}
			var key K
			var value V
			if err = msgpack.Unmarshal(keyBlob, &key); err != nil {
				return
			}
			if err = msgpack.Unmarshal(valueBlob, &value); err != nil {
				return
			}
			if !yield(key, value) {
				return
			}
		}
	}
}
func (s *Store[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		rows, err := s.db.Query("SELECT kvl_key FROM kvl_store")
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			keyBlob := []byte{}
			if err := rows.Scan(&keyBlob); err != nil {
				return
			}
			var key K
			if err = msgpack.Unmarshal(keyBlob, &key); err != nil {
				return
			}
			if !yield(key) {
				return
			}
		}
	}
}
func (s *Store[K, V]) Values() iter.Seq[V] {
	return func(yield func(V) bool) {
		rows, err := s.db.Query("SELECT kvl_value FROM kvl_store")
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			valueBlob := []byte{}
			if err := rows.Scan(&valueBlob); err != nil {
				return
			}
			var value V
			if err = msgpack.Unmarshal(valueBlob, &value); err != nil {
				return
			}
			if !yield(value) {
				return
			}
		}
	}
}
