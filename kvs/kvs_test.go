package kvs_test

import (
	"database/sql"
	"log"
	"testing"
	"time"

	"github.com/t0k4r/kvl/kvs"
	_ "modernc.org/sqlite"
)

func TestXxx(t *testing.T) {
	db, err := sql.Open("sqlite", "test.db")
	if err != nil {
		log.Fatal(err)
		t.FailNow()
	}
	cache, err := kvs.Open[string](db, time.Minute)
	if err != nil {
		log.Fatal(err)
		t.FailNow()
	}
	err = cache.Store([]byte("hello"), "world")
	if err != nil {
		log.Fatal(err)
		t.FailNow()
	}
	err = cache.Store([]byte("general"), "kenobi")
	if err != nil {
		log.Fatal(err)
		t.FailNow()
	}
	v, ok := cache.Load([]byte("general"))
	if !ok || v != "kenobi" {
		log.Fatal("kenobi1")
		t.FailNow()
	}
}
