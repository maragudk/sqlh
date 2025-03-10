package sql

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/maragudk/goqite"
	_ "github.com/mattn/go-sqlite3"
	"maragu.dev/errors"
)

type Helper struct {
	DB    *sqlx.DB
	JobsQ *goqite.Queue
	log   *slog.Logger
	path  string
}

type NewHelperOptions struct {
	Log  *slog.Logger
	Path string
}

// NewHelper with the given options.
// If no logger is provided, logs are discarded.
func NewHelper(opts NewHelperOptions) *Helper {
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}

	// - Set WAL mode (not strictly necessary each time because it's persisted in the database, but good for first run)
	// - Set busy timeout, so concurrent writers wait on each other instead of erroring immediately
	// - Enable foreign key checks
	opts.Path += "?_journal=WAL&_timeout=5000&_fk=true"

	return &Helper{
		log:  opts.Log,
		path: opts.Path,
	}
}

func (h *Helper) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h.log.Info("Starting database", "path", h.path)

	var err error
	h.DB, err = sqlx.ConnectContext(ctx, "sqlite3", h.path)
	if err != nil {
		return err
	}

	return nil
}

// InTransaction runs callback in a transaction, and makes sure to handle rollbacks, commits etc.
func (h *Helper) InTransaction(ctx context.Context, callback func(tx *Tx) error) (err error) {
	tx, err := h.DB.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return errors.Wrap(err, "error beginning transaction")
	}
	defer func() {
		if rec := recover(); rec != nil {
			err = rollback(tx, errors.Newf("panic: %v", rec))
		}
	}()
	if err := callback(&Tx{Tx: tx}); err != nil {
		return rollback(tx, err)
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "error committing transaction")
	}

	return nil
}

// rollback a transaction, handling both the original error and any transaction rollback errors.
func rollback(tx *sqlx.Tx, err error) error {
	if txErr := tx.Rollback(); txErr != nil {
		return errors.Wrap(err, "error rolling back transaction after error (transaction error: %v), original error", txErr)
	}
	return err
}

func (h *Helper) Ping(ctx context.Context) error {
	return h.InTransaction(ctx, func(tx *Tx) error {
		return tx.Exec(ctx, `select 1`)
	})
}

func (h *Helper) Select(ctx context.Context, dest any, query string, args ...any) error {
	return h.DB.SelectContext(ctx, dest, query, args...)
}

func (h *Helper) Get(ctx context.Context, dest any, query string, args ...any) error {
	return h.DB.GetContext(ctx, dest, query, args...)
}

func (h *Helper) Exec(ctx context.Context, query string, args ...any) error {
	_, err := h.DB.ExecContext(ctx, query, args...)
	return err
}

type Tx struct {
	Tx *sqlx.Tx
}

func (t *Tx) Select(ctx context.Context, dest any, query string, args ...any) error {
	return t.Tx.SelectContext(ctx, dest, query, args...)
}

func (t *Tx) Get(ctx context.Context, dest any, query string, args ...any) error {
	return t.Tx.GetContext(ctx, dest, query, args...)
}

func (t *Tx) Exec(ctx context.Context, query string, args ...any) error {
	_, err := t.Tx.ExecContext(ctx, query, args...)
	return err
}

var ErrNoRows = sql.ErrNoRows
