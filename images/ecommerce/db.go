package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ywacch/cloud9/pkg/templateapp/consistency"
	"github.com/ywacch/cloud9/pkg/templateapp/postgres"
	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
	"github.com/ywacch/cloud9/pkg/templateapp/trace"
)

var (
	ErrNotFound          = errors.New("not found")
	ErrInsufficientStock = errors.New("insufficient stock")
)

type Product struct {
	ID    int     `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
	Stock int     `json:"stock"`
}

type Order struct {
	ID        int       `json:"id"`
	ProductID int       `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type readSplitRouter interface {
	QueryRead(
		ctx context.Context,
		clientID string,
		build func(readTokenExpr string) (sql string, args []any),
		scan func(row pgx.Row, replayToken *string) error,
	) (readrouter.Route, string, error)
	QueryRows(
		ctx context.Context,
		clientID string,
		run func(ctx context.Context, q readrouter.Querier) error,
	) (readrouter.Route, string, error)
	AfterWrite(clientID string, t readrouter.ConsistencyToken)
}

type DB struct {
	pool      *pgxpool.Pool
	readPools []*pgxpool.Pool
	router    readSplitRouter
	engine    readrouter.Engine
}

func newDB(pool *pgxpool.Pool, readPools []*pgxpool.Pool, router readSplitRouter) *DB {
	return &DB{
		pool:      pool,
		readPools: readPools,
		router:    router,
		engine:    readrouter.PostgresEngine{},
	}
}

func (db *DB) Close() {
	for _, p := range db.readPools {
		if p != nil {
			p.Close()
		}
	}
}

func (db *DB) ReplicaSamplesSource() consistency.ReplicaSamplesSource {
	if db == nil {
		return nil
	}
	if ptr, ok := db.router.(*readrouter.ReadRouter); ok {
		return ptr
	}
	return nil
}

func routerActive(r readSplitRouter) bool {
	if r == nil {
		return false
	}
	if ptr, ok := r.(*readrouter.ReadRouter); ok {
		return ptr != nil
	}
	return true
}

func (db *DB) queryRows(ctx context.Context, run func(context.Context, readrouter.Querier) error) (readrouter.Route, string, error) {
	if !routerActive(db.router) {
		err := run(ctx, db.pool)
		return readrouter.RouteLeader, "postgres", err
	}
	return db.router.QueryRows(ctx, trace.ClientID(ctx), run)
}

func (db *DB) recordWriteToken(ctx context.Context, clientID string) error {
	if !routerActive(db.router) || clientID == "" {
		return nil
	}
	var raw string
	if err := db.pool.QueryRow(ctx, "SELECT "+db.engine.WriteTokenExpr()).Scan(&raw); err != nil {
		return err
	}
	tok, err := db.engine.ParseToken(raw)
	if err != nil {
		return err
	}
	db.router.AfterWrite(clientID, tok)
	return nil
}

func getProductQuery(tokenExpr string) string {
	if tokenExpr == "" {
		return `SELECT id, name, price::float8, stock FROM products WHERE id=$1`
	}
	return fmt.Sprintf(`SELECT id, name, price::float8, stock, %s FROM products WHERE id=$1`, tokenExpr)
}

func getOrderQuery(tokenExpr string) string {
	if tokenExpr == "" {
		return `SELECT id, product_id, quantity, status, created_at FROM orders WHERE id=$1`
	}
	return fmt.Sprintf(`SELECT id, product_id, quantity, status, created_at, %s FROM orders WHERE id=$1`, tokenExpr)
}

func (db *DB) InitSchema(ctx context.Context) error {
	return postgres.Observe(ctx, "init_schema", "postgres", func() error {
		_, err := db.pool.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS products (
				id         SERIAL PRIMARY KEY,
				name       TEXT NOT NULL,
				price      NUMERIC(10,2) NOT NULL DEFAULT 0,
				stock      INT NOT NULL DEFAULT 0
			);

			CREATE TABLE IF NOT EXISTS orders (
				id          SERIAL PRIMARY KEY,
				product_id  INT NOT NULL REFERENCES products(id),
				quantity    INT NOT NULL DEFAULT 1,
				status      TEXT NOT NULL DEFAULT 'pending',
				created_at  TIMESTAMPTZ DEFAULT NOW()
			);
		`)
		return err
	})
}

func (db *DB) SeedIfEmpty(ctx context.Context) error {
	return postgres.Observe(ctx, "seed_if_empty", "postgres", func() error {
		var count int
		if err := db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM products`).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			return nil
		}

		start := time.Now()
		slog.Info("seeding ecommerce data (this takes ~5 seconds on first run)...")

		if _, err := db.pool.Exec(ctx, `
			INSERT INTO products (name, price, stock)
			SELECT
				(ARRAY[
					'Wireless', 'Premium', 'Essential', 'Pro', 'Ultra',
					'Classic', 'Compact', 'Advanced', 'Elite', 'Portable'
				])[1 + (random() * 9)::int]
				|| ' ' ||
				(ARRAY[
					'Headphones', 'Keyboard', 'Monitor', 'Backpack', 'Charger',
					'Speaker', 'Mouse', 'Webcam', 'Microphone', 'Hub',
					'Cable', 'Stand', 'Case', 'Adapter', 'Dock',
					'Tablet', 'Watch', 'Lamp', 'Drive', 'Router'
				])[1 + (random() * 19)::int]
				|| ' ' || i,
				ROUND((4.99 + random() * 495.00)::numeric, 2),
				10000
			FROM generate_series(1, 1000) AS i
		`); err != nil {
			return fmt.Errorf("seed products: %w", err)
		}

		if _, err := db.pool.Exec(ctx, `
			INSERT INTO orders (product_id, quantity, status, created_at)
			SELECT
				1 + (random() * 999)::int,
				1 + (random() * 4)::int,
				'fulfilled',
				NOW() - (random() * interval '30 days')
			FROM generate_series(1, 50000)
		`); err != nil {
			return fmt.Errorf("seed orders: %w", err)
		}

		var productCount, orderCount int
		db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM products`).Scan(&productCount)
		db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM orders`).Scan(&orderCount)

		slog.Info("seeded ecommerce data",
			"products", productCount,
			"orders", orderCount,
			"duration", time.Since(start).String(),
		)
		return nil
	})
}

func (db *DB) ListProducts(ctx context.Context) ([]Product, error) {
	var out []Product
	err := postgres.ObserveTarget(ctx, "list_products", func() (string, error) {
		_, target, err := db.queryRows(ctx, func(ctx context.Context, q readrouter.Querier) error {
			rows, err := q.Query(ctx, `SELECT id, name, price::float8, stock FROM products ORDER BY id`)
			if err != nil {
				return err
			}
			defer rows.Close()
			out = out[:0]
			for rows.Next() {
				var p Product
				if err := rows.Scan(&p.ID, &p.Name, &p.Price, &p.Stock); err != nil {
					return err
				}
				out = append(out, p)
			}
			return rows.Err()
		})
		return target, err
	})
	return out, err
}

// ProductStockForVerify reads stock for cache consistency checks on the primary only.
func (db *DB) ProductStockForVerify(ctx context.Context, id int) (int, error) {
	var stock int
	err := db.pool.QueryRow(ctx, `SELECT stock FROM products WHERE id=$1`, id).Scan(&stock)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	return stock, err
}

func (db *DB) GetProduct(ctx context.Context, id int) (Product, error) {
	if !routerActive(db.router) {
		var p Product
		err := postgres.Observe(ctx, "get_product", "postgres", func() error {
			var err error
			p, err = db.getProductLeader(ctx, id)
			return err
		})
		return p, err
	}

	clientID := trace.ClientID(ctx)
	var p Product
	err := postgres.ObserveTarget(ctx, "get_product", func() (string, error) {
		_, target, err := db.router.QueryRead(ctx, clientID,
			func(tokenExpr string) (string, []any) {
				return getProductQuery(tokenExpr), []any{id}
			},
			func(row pgx.Row, replayToken *string) error {
				if replayToken != nil {
					return row.Scan(&p.ID, &p.Name, &p.Price, &p.Stock, replayToken)
				}
				return row.Scan(&p.ID, &p.Name, &p.Price, &p.Stock)
			},
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return target, ErrNotFound
		}
		return target, err
	})
	if errors.Is(err, ErrNotFound) {
		return Product{}, ErrNotFound
	}
	return p, err
}

func (db *DB) getProductLeader(ctx context.Context, id int) (Product, error) {
	var p Product
	err := db.pool.QueryRow(ctx, getProductQuery(""), id).Scan(&p.ID, &p.Name, &p.Price, &p.Stock)
	if errors.Is(err, pgx.ErrNoRows) {
		return Product{}, ErrNotFound
	}
	return p, err
}

func (db *DB) CheckoutOptimistic(ctx context.Context, productID, quantity int) (int, error) {
	var orderID int
	err := postgres.Observe(ctx, "checkout_optimistic", "postgres", func() error {
		tx, err := db.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		var stock int
		err = tx.QueryRow(ctx,
			`UPDATE products SET stock = stock - $2 WHERE id = $1 AND stock >= $2 RETURNING stock`,
			productID, quantity,
		).Scan(&stock)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrInsufficientStock
		}
		if err != nil {
			return err
		}

		err = tx.QueryRow(ctx,
			`INSERT INTO orders (product_id, quantity, status) VALUES ($1, $2, 'fulfilled') RETURNING id`,
			productID, quantity,
		).Scan(&orderID)
		if err != nil {
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return db.recordWriteToken(ctx, trace.ClientID(ctx))
	})
	return orderID, err
}

func (db *DB) CheckoutPessimistic(ctx context.Context, productID, quantity int) (int, error) {
	var orderID int
	err := postgres.Observe(ctx, "checkout_pessimistic", "postgres", func() error {
		tx, err := db.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		var stock int
		err = tx.QueryRow(ctx, `SELECT stock FROM products WHERE id = $1 FOR UPDATE`, productID).Scan(&stock)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if stock < quantity {
			return ErrInsufficientStock
		}

		if _, err := tx.Exec(ctx, `UPDATE products SET stock = stock - $2 WHERE id = $1`, productID, quantity); err != nil {
			return err
		}
		err = tx.QueryRow(ctx,
			`INSERT INTO orders (product_id, quantity, status) VALUES ($1, $2, 'fulfilled') RETURNING id`,
			productID, quantity,
		).Scan(&orderID)
		if err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return db.recordWriteToken(ctx, trace.ClientID(ctx))
	})
	return orderID, err
}

func (db *DB) InsertPendingOrder(ctx context.Context, productID, quantity int) (int, error) {
	var orderID int
	err := postgres.Observe(ctx, "insert_pending_order", "postgres", func() error {
		err := db.pool.QueryRow(ctx,
			`INSERT INTO orders (product_id, quantity, status) VALUES ($1, $2, 'pending') RETURNING id`,
			productID, quantity,
		).Scan(&orderID)
		if err != nil {
			return err
		}
		return db.recordWriteToken(ctx, trace.ClientID(ctx))
	})
	return orderID, err
}

func (db *DB) GetOrder(ctx context.Context, id int) (Order, error) {
	if !routerActive(db.router) {
		var o Order
		err := postgres.Observe(ctx, "get_order", "postgres", func() error {
			var err error
			o, err = db.getOrderLeader(ctx, id)
			return err
		})
		return o, err
	}

	clientID := trace.ClientID(ctx)
	var o Order
	err := postgres.ObserveTarget(ctx, "get_order", func() (string, error) {
		_, target, err := db.router.QueryRead(ctx, clientID,
			func(tokenExpr string) (string, []any) {
				return getOrderQuery(tokenExpr), []any{id}
			},
			func(row pgx.Row, replayToken *string) error {
				if replayToken != nil {
					return row.Scan(&o.ID, &o.ProductID, &o.Quantity, &o.Status, &o.CreatedAt, replayToken)
				}
				return row.Scan(&o.ID, &o.ProductID, &o.Quantity, &o.Status, &o.CreatedAt)
			},
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return target, ErrNotFound
		}
		return target, err
	})
	if errors.Is(err, ErrNotFound) {
		return Order{}, ErrNotFound
	}
	return o, err
}

func (db *DB) getOrderLeader(ctx context.Context, id int) (Order, error) {
	var o Order
	err := db.pool.QueryRow(ctx, getOrderQuery(""), id).Scan(&o.ID, &o.ProductID, &o.Quantity, &o.Status, &o.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	return o, err
}

func (db *DB) ListProductIDs(ctx context.Context, limit int) ([]int, error) {
	var ids []int
	err := postgres.ObserveTarget(ctx, "list_product_ids", func() (string, error) {
		_, target, err := db.queryRows(ctx, func(ctx context.Context, q readrouter.Querier) error {
			rows, err := q.Query(ctx, `SELECT id FROM products ORDER BY id LIMIT $1`, limit)
			if err != nil {
				return err
			}
			defer rows.Close()
			ids = ids[:0]
			for rows.Next() {
				var id int
				if err := rows.Scan(&id); err != nil {
					return err
				}
				ids = append(ids, id)
			}
			return rows.Err()
		})
		return target, err
	})
	return ids, err
}

func (db *DB) FulfillOrder(ctx context.Context, orderID int) error {
	return postgres.Observe(ctx, "fulfill_order", "postgres", func() error {
		_, err := db.pool.Exec(ctx, `UPDATE orders SET status='fulfilled' WHERE id=$1`, orderID)
		return err
	})
}

func (db *DB) DecrementStock(ctx context.Context, productID, quantity int) error {
	return postgres.Observe(ctx, "decrement_stock", "postgres", func() error {
		tag, err := db.pool.Exec(ctx,
			`UPDATE products SET stock = stock - $2 WHERE id = $1 AND stock >= $2`,
			productID, quantity,
		)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrInsufficientStock
		}
		return nil
	})
}
