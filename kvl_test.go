package kvl_test

import (
	"log/slog"
	"testing"

	"github.com/t0k4r/kvl"
)

func TestXxx(t *testing.T) {
	str, err := kvl.Open[string]("github.com.t0k4r.kvl.test")
	if err != nil {
		slog.Error(err.Error())
		t.Fail()
	}
	err = str.Delete("ok")
	if err != nil {
		slog.Error(err.Error())
		t.Fail()
	}
	_, ok := str.Load("ok")
	if ok {
		slog.Error(err.Error())
		t.Fail()
	}
	err = str.Store("ok", "abc")
	if err != nil {
		slog.Error(err.Error())
		t.Fail()
	}
	value, ok := str.Load("ok")
	if !ok || value != "abc" {
		t.Fail()
	}
}
