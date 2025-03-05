package sqltest

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/maragudk/goqite"

	"maragu.dev/sqlh/sql"
)

type testWriter struct {
	t *testing.T
}

func (t *testWriter) Write(p []byte) (n int, err error) {
	t.t.Log(string(p))
	return len(p), nil
}

// NewHelper for testing.
func NewHelper(t *testing.T) *sql.Helper {
	t.Helper()

	cleanup(t)
	t.Cleanup(func() {
		cleanup(t)
	})

	sqlHelper := sql.NewHelper(sql.NewHelperOptions{
		Log:  slog.New(slog.NewTextHandler(&testWriter{t: t}, nil)),
		Path: "test.db",
	})
	if err := sqlHelper.Connect(); err != nil {
		t.Fatal(err)
	}

	if err := sqlHelper.MigrateUp(context.Background()); err != nil {
		t.Fatal(err)
	}

	q := goqite.New(goqite.NewOpts{
		DB:   sqlHelper.DB.DB,
		Name: "jobs",
	})
	sqlHelper.JobsQ = q

	return sqlHelper
}

func cleanup(t *testing.T) {
	t.Helper()

	files, err := filepath.Glob("test.db*")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}
	}
}
