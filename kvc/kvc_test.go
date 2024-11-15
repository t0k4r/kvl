package kvc_test

import (
	"database/sql"
	"log"
	"testing"
	"time"

	"github.com/t0k4r/kvl/kvc"
	_ "modernc.org/sqlite"
)

func TestXxx(t *testing.T) {
	db, err := sql.Open("sqlite", "test.db")
	if err != nil {
		log.Fatal(err)
		t.FailNow()
	}
	cache, err := kvc.Open[string](db, time.Minute)
	if err != nil {
		log.Fatal(err)
		t.FailNow()
	}
	err = cache.Store([]byte("hello"), "world", time.Minute*2)
	if err != nil {
		log.Fatal(err)
		t.FailNow()
	}
	err = cache.Store([]byte("general"), "kenobi", time.Second*20)
	if err != nil {
		log.Fatal(err)
		t.FailNow()
	}
	v, ok := cache.Load([]byte("general"))
	if !ok || v != "kenobi" {
		log.Fatal("kenobi1")
		t.FailNow()
	}
	time.Sleep(time.Minute)
	v, loaded := cache.LoadOrStore([]byte("general"), "kenobi", time.Second*20)
	if loaded {
		log.Fatal("kenobi2")
		log.Fatal(err)
		t.FailNow()
	}
	v, loaded = cache.LoadOrStore([]byte("general"), "kenobi", time.Second*20)
	if !loaded || v != "kenobi" {
		log.Fatalf("kenobi3 %v", loaded)
		log.Fatal(err)
		t.FailNow()
	}
}
