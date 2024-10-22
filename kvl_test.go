package kvl_test

import (
	"log/slog"
	"testing"

	"github.com/t0k4r/kvl"
)

func TestXxx(t *testing.T) {
	s, err := kvl.Open[string, string]("github.com.t0k4r.kvl.test")
	if err != nil {
		slog.Error(err.Error())
		t.Fail()
	}
	defer s.Close()
	err = s.Delete("ok")
	if err != nil {
		slog.Error(err.Error())
		t.Fail()
	}
	_, ok := s.Load("ok")
	if ok {
		slog.Error(err.Error())
		t.Fail()
	}
	err = s.Store("ok", "abc")
	if err != nil {
		slog.Error(err.Error())
		t.Fail()
	}
	value, ok := s.Load("ok")
	if !ok || value != "abc" {
		t.Fail()
	}
}
