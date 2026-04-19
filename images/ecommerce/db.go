package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

type DB struct {
	pool *pgxpool.Pool
}

func newDB(pool *pgxpool.Pool) *DB {
	return &DB{pool: pool}
}

func (db *DB) InitSchema(ctx context.Context) error {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("init_schema").Observe(time.Since(start).Seconds()) }()
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
}

func (db *DB) SeedIfEmpty(ctx context.Context) error {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("seed_if_empty").Observe(time.Since(start).Seconds()) }()

	var count int
	if err := db.pool.QueryRow(ctx, `SELECT COUNT(*) FROM products`).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	slog.Info("seeding ecommerce data (this takes ~5 seconds on first run)...")

	// 1. Seed 1,000 products with realistic categories and varied pricing (~1s)
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

	// 2. Seed 50,000 historical fulfilled orders over 30 days (~3s)
	//    Gives the orders table realistic size and the /orders/{id} endpoint real data.
	//    Quantity 1-5, product IDs 1-1000, spread over past month.
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
}

func (db *DB) ListProducts(ctx context.Context) ([]Product, error) {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("list_products").Observe(time.Since(start).Seconds()) }()
	rows, err := db.pool.Query(ctx, `SELECT id, name, price::float8, stock FROM products ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ID, &p.Name, &p.Price, &p.Stock); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (db *DB) GetProduct(ctx context.Context, id int) (Product, error) {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("get_product").Observe(time.Since(start).Seconds()) }()
	var p Product
	err := db.pool.QueryRow(ctx,
		`SELECT id, name, price::float8, stock FROM products WHERE id=$1`,
		id,
	).Scan(&p.ID, &p.Name, &p.Price, &p.Stock)
	if errors.Is(err, pgx.ErrNoRows) {
		return Product{}, ErrNotFound
	}
	return p, err
}

func (db *DB) CheckoutOptimistic(ctx context.Context, productID, quantity int) (int, error) {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("checkout_optimistic").Observe(time.Since(start).Seconds()) }()

	tx, err := db.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var stock int
	err = tx.QueryRow(ctx,
		`UPDATE products SET stock = stock - $2 WHERE id = $1 AND stock >= $2 RETURNING stock`,
		productID, quantity,
	).Scan(&stock)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrInsufficientStock
	}
	if err != nil {
		return 0, err
	}

	var orderID int
	err = tx.QueryRow(ctx,
		`INSERT INTO orders (product_id, quantity, status) VALUES ($1, $2, 'fulfilled') RETURNING id`,
		productID, quantity,
	).Scan(&orderID)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return orderID, nil
}

func (db *DB) CheckoutPessimistic(ctx context.Context, productID, quantity int) (int, error) {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("checkout_pessimistic").Observe(time.Since(start).Seconds()) }()

	tx, err := db.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	var stock int
	err = tx.QueryRow(ctx, `SELECT stock FROM products WHERE id = $1 FOR UPDATE`, productID).Scan(&stock)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	if stock < quantity {
		return 0, ErrInsufficientStock
	}

	if _, err := tx.Exec(ctx, `UPDATE products SET stock = stock - $2 WHERE id = $1`, productID, quantity); err != nil {
		return 0, err
	}
	var orderID int
	err = tx.QueryRow(ctx,
		`INSERT INTO orders (product_id, quantity, status) VALUES ($1, $2, 'fulfilled') RETURNING id`,
		productID, quantity,
	).Scan(&orderID)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return orderID, nil
}

func (db *DB) InsertPendingOrder(ctx context.Context, productID, quantity int) (int, error) {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("insert_pending_order").Observe(time.Since(start).Seconds()) }()
	var orderID int
	err := db.pool.QueryRow(ctx,
		`INSERT INTO orders (product_id, quantity, status) VALUES ($1, $2, 'pending') RETURNING id`,
		productID, quantity,
	).Scan(&orderID)
	return orderID, err
}

func (db *DB) GetOrder(ctx context.Context, id int) (Order, error) {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("get_order").Observe(time.Since(start).Seconds()) }()
	var o Order
	err := db.pool.QueryRow(ctx,
		`SELECT id, product_id, quantity, status, created_at FROM orders WHERE id=$1`,
		id,
	).Scan(&o.ID, &o.ProductID, &o.Quantity, &o.Status, &o.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	return o, err
}

func (db *DB) FulfillOrder(ctx context.Context, orderID int) error {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("fulfill_order").Observe(time.Since(start).Seconds()) }()
	_, err := db.pool.Exec(ctx, `UPDATE orders SET status='fulfilled' WHERE id=$1`, orderID)
	return err
}

func (db *DB) DecrementStock(ctx context.Context, productID, quantity int) error {
	start := time.Now()
	defer func() { dbQueryDuration.WithLabelValues("decrement_stock").Observe(time.Since(start).Seconds()) }()
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
}
