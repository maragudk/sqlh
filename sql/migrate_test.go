package sql_test

import (
	"context"
	"testing"

	"maragu.dev/is"

	"maragu.dev/sqlh/sqltest"
)

func TestHelper_Migrate(t *testing.T) {
	t.Run("can migrate down and back up", func(t *testing.T) {
		h := sqltest.NewHelper(t)

		err := h.MigrateDown(context.Background())
		is.NotError(t, err)

		err = h.MigrateUp(context.Background())
		is.NotError(t, err)

		var version string
		err = h.Get(context.Background(), &version, `select version from migrations`)
		is.NotError(t, err)
		is.True(t, len(version) > 0)
	})
}
